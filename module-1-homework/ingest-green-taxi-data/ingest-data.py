import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
from urllib.parse import urlparse
import requests
import gzip
import shutil

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Extract filename from URL
    url_path = urlparse(url).path
    csv_name = os.path.basename(url_path)

    # If the file is gzipped, remove the '.gz' extension
    if csv_name.endswith('.csv.gz'):
        csv_name = csv_name.replace('.gz', '')

    # Download the file using requests
    response = requests.get(url)

    # Save compressed content to a file
    compressed_file_path = csv_name + '.gz'
    with open(compressed_file_path, 'wb') as compressed_file:
        compressed_file.write(response.content)

    # Extract the compressed file
    extracted_file_path = csv_name
    with gzip.open(compressed_file_path, 'rb') as f_in:
        with open(extracted_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Read the extracted file using pandas
    df_iter = pd.read_csv(extracted_file_path, iterator=True, chunksize=100000)

    # specify type of database 'postgresql', then user:password@port:port/databasename
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    # press shift + tab to see 'help' on a parameter
    # we pass in iterator & chunk size so that it reads the table in to postgres in smaller chunks or it would get stuck

    df = next(df_iter)
    # next() method returns the next 'chunk' after an iterator has been declared

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    # needs to be ran again after we've converted to a df 

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    # to_sql method will go to the database and insert all the rows
    # also specify the parameter if_exists - if the database exists, it will 'replace' it 

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 

        try:
            t_start = time() #returns the current timestamp at the begining

            df = next(df_iter) #append next chunk 

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime) #transform datatype to timestamp for these columns
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)# ^
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time() #current timestamp at the end 

            print('inserted another chunk, took %.3f second' % (t_end - t_start)) #%.3f = time will be a float with 3 decimals 

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break # break the loop when the end of the file is reached


if __name__ == '__main__':

    # copy this from https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    # this line is the description of the file 

    # then we need to add in the arguments that we need - this is so we don't hardcode the variables into the script making it reusable for others 
    # list the variables you'll need in comment form first to help you build the arguments 

    # we need user, password, host, port, database name, table name, url of the csv 

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args() # this line parses the arguements

    main(args) # we need to call the main function & parse in the args


# CLI prompt:

# enter the url variable first on it's own
    
# URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

# python ingest-data.py --user=root --password=root --host=localhost --port=5433 --db=ny_taxi --table_name=green_taxi_data --url=${URL}