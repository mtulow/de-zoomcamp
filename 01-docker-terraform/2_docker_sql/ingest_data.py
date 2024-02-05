#!/usr/bin/env python
# coding: utf-8

import os
import argparse

import time
import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(level=logging.DEBUG)


def fetch_dataset(service: str, year: int, month: int, ext: str):
    """Fetch the dataset from the internet.

    Args:
        service (str): name of the taxi service, e.g. green or yellow
        year (int): year of the data
        month (int): month of the data
        ext (str): extension of the data file

    Returns:
        str: path to the fetched dataset
    """
    # Download data file by file type
    if ext == '.csv':
        # Set the download arguments
        url = f'wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{service}_tripdata_{year}-{month:02d}.csvs.gz'
        file_name = os.path.basename(url).replace('.gz', '')
        compressed_file = file_name + '.gz'
    
        # Download the file, if it doesn't exist
        if not os.path.exists(compressed_file):
            # If the file doesn't exist, download it
            logging.info(f"Downloading data file {os.path.basename(url)} ...")
            cmd = f"wget {url} -O {compressed_file}"
            logging.debug(cmd)
            os.system(cmd)
            
            # # File already compressed, skip the compression step
            # logging.info(f"Decompressing data file {file_name} ...")
            # cmd = f"gzip -d {compressed_file}"
            # logging.debug(cmd)
            # os.system(cmd)
        else:
            # If the file exists, skip the download
            logging.info(f"Data file {file_name} already exists, skipping download ...")
        return file_name, compressed_file
    
    elif ext == '.parquet':
        # Set the download arguments
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'
        file_name = os.path.basename(url)
        compressed_file = file_name + '.gz'
        
        # Download the file, if it doesn't exist
        if not os.path.exists(file_name):
            # If the file doesn't exist, download it
            logging.info(f"Downloading data file {os.path.basename(url)} ...")
            cmd = f"wget {url} -O {file_name}"
            logging.debug(cmd)
            os.system(cmd)
            
            # # Compress the file
            # logging.info(f"Compressing data file {file_name} ...")
            # cmd = f"gzip {file_name}"
            # logging.debug(cmd)
            # os.system(cmd)
            
        else:
            # If the file exists, skip the download
            logging.info(f"Data file {file_name} already exists, skipping download ...")
        return file_name, compressed_file
    
    else:
        raise ValueError('Extension must be either `.csv` or `.parquets`')

def upload_dataset(file_path: str, table_name: str, schema: str, engine: sa.engine.Engine, ext: str):
    """Upload the dataset to the postgres database.

    Args:
        file_path (str): path to the compressed data file
        table_name (str): name of the table where we will write the results to
        schema (str): name of the schema where the table is located
        engine (create_engine): a sqlalchemy engine object
        ext (str): extension of the data file
    """
    # Set the datetime columns
    if 'yellow' in file_path:
        date_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    elif 'green' in file_path:
        date_cols = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    else:
        raise ValueError('File name must contain `yellow` or `green`.')
    
    # Ingest the data into the postgres database table by file type
    if ext == '.csv':
        # Read the csv file in chunks
        df_iter = pd.read_csv(file_path, iterator=True,
                              chunksize=100_000, parse_dates=date_cols)

        # Read the first chunk
        df = next(df_iter)
        
        # Convert the datetime columns to datetime objects
        df[date_cols] = df[date_cols].apply(pd.to_datetime)

        # Create/Recreate the table in the database
        df.head(n=0).to_sql(name=table_name, con=engine, schema=schema, if_exists='replace')

        # Write the first chunk to the database
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists='append', index='trip_id')

        while True:

            try:
                t_start = time.perf_counter()
                
                df = next(df_iter)

                df[date_cols] = df[date_cols].apply(pd.to_datetime)

                df.to_sql(name=table_name, con=engine, schema=schema, if_exists='append', index='trip_id')

                t_end = time.perf_counter()

                logging.info('inserted another chunk, took %.3f second' % (t_end - t_start))

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break

    elif ext == '.parquet':
        df = pd.read_parquet(file_path,)

        # Create the schema if it doesn't exist
        logging.info(f"Creating schema `{schema}` if it doesn't exist ...")
        with engine.connect() as conn:
            conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS {schema};'))
            # conn.commit()

        # Write the dataframe to the database
        logging.info(f"Writing data to the table `{schema}`.`{table_name}` ...")
        df.to_sql(name=table_name, con=engine, schema=schema, index='trip_id',
                  if_exists='replace', chunksize=100_000, method='multi')
        
    else:
        raise ValueError('Extension must be either `.csv` or `.parquet`')

def main(params):
    # Get the data file parameters
    service = params.service
    year = params.year
    month = params.month
    ext = params.ext

    # Fetch the dataset
    file_name, compressed_file = fetch_dataset(service, year, month, ext)

    # Get the database parameters
    host = params.host
    port = params.port
    db = params.db

    # Connect to the postgres database
    engine = create_engine(f'postgresql://{params.user}:{params.password}@{host}:{port}/{db}')
    
    # Get the database table parameters
    schema = params.schema
    table_name = f'{service}_taxi_trips' if params.table_name is None \
            else params.table_name
    
    # Ingest the data into the postgres database table
    upload_dataset(file_name, table_name, schema, engine, ext)

    # Remove the files after ingesting the data
    logging.info(f"Removing files `{file_name}` & `{compressed_file}` ...")
    os.remove(file_name)
    os.remove(compressed_file)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # Data file parameters
    parser.add_argument('--service', required=True, \
                        help='name of the taxi service, e.g. green or yellow')
    parser.add_argument('--year', required=True, type=int, \
                        help='year of the data')
    parser.add_argument('--month', required=True, type=int, \
                        help='month of the data')
    parser.add_argument('--ext', required=False, default='.parquet', \
                        help='extension of the data file')
    # Database credentials
    parser.add_argument('--user', required=True, \
                        help='user name for postgres')
    parser.add_argument('--password', required=True, \
                        help='password for postgres')
    parser.add_argument('--host', required=True, \
                        help='host for postgres')
    parser.add_argument('--port', required=True, \
                        help='port for postgres')
    parser.add_argument('--db', required=True, \
                        help='database name for postgres')
    parser.add_argument('--schema', required=False, default='dev', \
                        help='name of the schema where the table is located')
    # Database table parameters
    parser.add_argument('--table_name', required=False, default=None,
                        help='name of the table where we will write the results to')
    

    args = parser.parse_args()

    main(args)
