from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import max
from constants import REV_ZIP_CODE_COL_NAME, LONGITUDE_COL_NAME, LATITUDE_COL_NAME
import math
import socket


def location_has_more_than_one_zip_code(df: DataFrame, groupby_col_name: str):
    """"
    """
    original_rows = df.count()
    df2 = df.groupBy(groupby_col_name).agg(max(REV_ZIP_CODE_COL_NAME).alias('Zip_code'))
    aggregated_rows = df2.count()

    print(f'Original rows: {original_rows}, Aggregated rows: {aggregated_rows}')

    return original_rows != aggregated_rows


def get_distance(lat1, lon1, lat2, lon2) -> float:
    # Radius of the Earth in kilometers
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c

    return round(distance, 3)


def clear_null_island_rows(df: DataFrame) -> DataFrame:
    return df.filter((df[LONGITUDE_COL_NAME] != 0) &
                     (df[LATITUDE_COL_NAME] != 0))


def get_private_ip():
    hostname = socket.gethostname()
    private_ip = socket.gethostbyname(hostname)
    print(f"Hostname: '{hostname}', Private IP: '{private_ip}'")
    return private_ip


def get_hdfs_subpath() -> str:
    return 'files'


def get_hdfs_path() -> str:
    port = 9000
    private_ip = get_private_ip()
    # return r"C:\Users\panos\Documents\PhD\courses\semester-4"
    return f'hdfs://{private_ip}:{port}/{get_hdfs_subpath()}'


def get_parquet_filepath():
    return f'{get_hdfs_path()}/parquet'

