import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):

    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    dfs = []

    months = [10, 11, 12]

    for month in months:
        file_name = f"green_tripdata_2020-{month:02d}.csv.gz"
        url = base_url + file_name
        dfs.append(pd.read_csv(url, sep=',', dtype=taxi_dtypes, parse_dates=parse_dates, compression='gzip'))

    merged_df = pd.concat(dfs, ignore_index=True)

    return merged_df
    