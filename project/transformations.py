from pyspark.sql import SparkSession
from utils import get_hdfs_path, get_parquet_filepath


def from_csv_to_parquet(dataset: str):
    """
    :param dataset:
    :return:
    """
    # Add this config if the code runs locally
    # .config("hadoop.home.dir", r"C:\Users\panos\Spark\spark-3.5.1-bin-hadoop3\hadoop") \
    spark = SparkSession.builder \
        .appName("YourAppName") \
        .getOrCreate()

    hdfs_path = get_hdfs_path()
    destination_path = get_parquet_filepath()

    df = spark.read.csv(path=f'{hdfs_path}/{dataset}', header=True, inferSchema=True)

    df.write.parquet(destination_path)  # .mode("overwrite")
    spark.stop()


def merge_files_into_one(file_name_1: str, file_name_2: str, final_name: str):
    spark = SparkSession.builder \
        .appName("YourAppName9346") \
        .getOrCreate()

    hdfs_path = get_hdfs_path()

    df1 = spark.read.csv(path=f'{hdfs_path}/{file_name_1}', header=True, inferSchema=True)
    df2 = spark.read.csv(path=f'{hdfs_path}/{file_name_2}', header=True, inferSchema=True)

    merged_df = df1.union(df2)

    # print(f'PAANOS number of merged rows: {merged_df.count()}')
    # merged_df.show(5)
    #
    # print(f'PAANOS before writing the file')
    # # .mode("overwrite")
    # try:
    #     merged_df.write.mode("overwrite").csv(f"{hdfs_path}/{final_name}", header=True)
    # except Exception as e:
    #     print(f'PAANOS HDFS error: {e}')
    # print(f'PAANOS after writing the file to: {hdfs_path}/{final_name}')

    print(f'PAANOS attempting to write it locally')
    merged_df.write.csv(f"./{final_name}", header=True)
    print(f'PAANOS after writing file locally to ./{final_name}')

    spark.stop()
