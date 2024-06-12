from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, to_timestamp, row_number, when, col, concat, lit, udf, count, avg, round, \
    sum, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
from utils import location_has_more_than_one_zip_code, get_hdfs_path, get_parquet_filepath
from constants import income_dataset_schema, rev_geocoding_dataset_schema, main_dataset_schema, \
    VICTIM_DESCENT_COL_NAME, WEAPON_USED_CODE_COL_NAME, LONGITUDE_COL_NAME, DATE_REPORTED_COL_NAME, \
    INC_ZIP_CODE_COL_NAME, REV_ZIP_CODE_COL_NAME, EST_MEDIAN_INCOME_COL_NAME, LATITUDE_COL_NAME, \
    Q4_PRECINCT_COL_NAME, AREA_COL_NAME, Q4_LATITUDE_COL_NAME, Q4_LONGITUDE_COL_NAME, Q4_DIVISION_COL_NAME, \
    TIMESTAMP_FORMAT, TIME_OCCURRED_COL_NAME, PREMISE_CODE_COL_NAME


LON_LAT_CONCAT_COL_NAME = 'lon_lat_concat'


def __get_sql_query__(table_name):
    date_reported_timestamp_col_name = 'date_reported_timestamp'
    year_col_name = 'year'
    month_col_name = 'month'
    crime_total_col_name = 'crime_total'
    ranking_col_name = 'ranking'

    nested_query = f"SELECT YEAR(`{date_reported_timestamp_col_name}`) AS `{year_col_name}`, " \
                   f"MONTH(`{date_reported_timestamp_col_name}`) AS `{month_col_name}`, " \
                   f"COUNT(*) AS `{crime_total_col_name}`, " \
                   f"ROW_NUMBER() OVER (PARTITION BY YEAR(`{date_reported_timestamp_col_name}`) ORDER BY COUNT(*) DESC) AS `{ranking_col_name}` " \
                   f"FROM {table_name} WHERE 1=1 " \
                   f"GROUP BY YEAR(`{date_reported_timestamp_col_name}`), MONTH(`{date_reported_timestamp_col_name}`)"
    query = f"SELECT `{year_col_name}`, `{month_col_name}`, `{crime_total_col_name}`, `{ranking_col_name}` " \
            f"FROM ({nested_query}) ranked_result " \
            f"WHERE `{ranking_col_name}` <= 3 " \
            f"ORDER BY `{year_col_name}` ASC, `{crime_total_col_name}` DESC"

    return query


def question_a_sql():
    hdfs_path = get_hdfs_path()
    filename = 'Crime_Data_all.csv'

    spark = SparkSession.builder \
        .appName("Query-1-SQL-API") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

    date_reported_timestamp_col_name = 'date_reported_timestamp'

    # df1 = spark.read.parquet(get_parquet_filepath())
    # localpath = r'C:\Users\panos\Documents\PhD\courses\semester-4/Crime_Data_all.csv'
    # df1 = spark.read.csv(f'{hdfs_path}/{filename}', header=True, inferSchema=True)

    df1 = spark.read.csv(f'{hdfs_path}/{filename}', header=True, inferSchema=True)

    df1 = df1.withColumn(date_reported_timestamp_col_name, to_timestamp(df1[DATE_REPORTED_COL_NAME], TIMESTAMP_FORMAT))

    table_name = 'crimeData'
    df1.createOrReplaceTempView(table_name)

    # Execute the query and get the results
    result_df = spark.sql(__get_sql_query__(table_name))

    # Show the result
    result_df.show(55)

    spark.stop()


def question_a():
    print('Question A - Running in DataFrame mode...')
    spark = SparkSession.builder \
        .appName("Query-1-DataFrame") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

    hdfs_path = get_hdfs_path()
    filename = 'Crime_Data_all.csv'
    # df = spark.read.parquet(get_parquet_filepath())
    df = spark.read.csv(f'{hdfs_path}/{filename}', header=True, inferSchema=True)

    df2 = df.withColumn("timestamp_panos", to_timestamp(df[DATE_REPORTED_COL_NAME], "MM/dd/yyyy hh:mm:ss a"))

    df3 = df2.withColumn("CRIME_YEAR", year(df2["timestamp_panos"]))
    df3 = df3.withColumn("CRIME_MONTH", month(df2["timestamp_panos"]))

    grouped_df = df3.groupBy("CRIME_YEAR", "CRIME_MONTH").count()

    window_spec = Window.partitionBy("CRIME_YEAR").orderBy(grouped_df["count"].desc())

    ranked_df = grouped_df.withColumn("rank", row_number().over(window_spec))

    limited_df = ranked_df.filter(ranked_df["rank"] <= 3)

    final_df = limited_df.withColumnRenamed("rank", "counter")

    final_ordered_df = final_df.orderBy("CRIME_YEAR", "counter")

    # Show the result
    final_ordered_df.show(55)

    spark.stop()


def question_b_rdd():
    """
    Column: 'Crm Cd Desc' contains comma separated description values
    which breaks the reading with RDDs when splitting the line to use
    the appropriate columns. This is the reason why we read the data
    using DataFrame. We first remove the offending column, then convert
    the DataFrame to RDD and then perform the rest of the functionality.
    :return:
    """
    print('Question B - Running in RDD mode...')

    spark = SparkSession.builder \
        .appName("Query-2-RDD") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

    hdfs_path = get_hdfs_path()
    filename = 'Crime_Data_all.csv'
    df = spark.read.csv(f'{hdfs_path}/{filename}', header=True, inferSchema=True)

    df = df.select(col(TIME_OCCURRED_COL_NAME), col(PREMISE_CODE_COL_NAME))

    rdd = df.rdd

    def get_part_of_day(time: int) -> str:
        if 500 <= time <= 1159:
            return 'morning (05:00-11:59)'
        elif 1200 <= time <= 1659:
            return 'afternoon (12:00-16:59)'
        elif 1700 <= time <= 2059:
            return 'night (17:00-20:59)'
        elif time >= 2100 or time <= 459:
            return 'midnight (21:00-04:59)'
        else:
            return 'unknown'

    data_rdd = rdd.filter(lambda line: line[PREMISE_CODE_COL_NAME] == 101) \
        .map(lambda line: (get_part_of_day(line[TIME_OCCURRED_COL_NAME]), 1)) \
        .groupByKey() \
        .mapValues(len) \
        .sortBy(lambda x: x[1], ascending=False)

    results = data_rdd.collect()

    for result in results:
        print(result)

    spark.stop()


def question_b():
    print('Question B - Running in DataFrame mode...')
    spark = SparkSession.builder \
        .appName("Query-2-DataFrame") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

    hdfs_path = get_hdfs_path()
    filename = 'Crime_Data_all.csv'
    df = spark.read.csv(f'{hdfs_path}/{filename}', header=True, inferSchema=True)

    df = df.withColumn('time_occ', col('TIME OCC').cast('int'))

    # 101 == STREET
    only_street_df = df.filter(df['Premis Cd'] == 101)

    enum_df = only_street_df.withColumn("time_of_day",
                                        when((col('time_occ') >= 500) & (col('time_occ') <= 1159),
                                             "morning (05:00-11:59)")
                                        .when((col('time_occ') >= 1200) & (col('time_occ') <= 1659),
                                              "afternoon (12:00-16:59)")
                                        .when((col('time_occ') >= 1700) & (col('time_occ') <= 2059),
                                              "night (17:00-20:59)")
                                        .when((col('time_occ') >= 2100) | (col('time_occ') <= 459),
                                              "midnight (21:00-04:59)")
                                        .otherwise("unknown")
                                        )

    madf = enum_df.groupBy('time_of_day').count()
    madf = madf.orderBy(madf["count"].desc())
    madf.show(truncate=False)

    spark.stop()


def question_c():
    """
    Problem:
    Find Victims' Descent and total number per descent category for year 2015, in
    regions (Zip codes) with the top and bottom 3 highest and lowest average income
    per household.

    Solution:
    We need to join income dataset with the revgeocoding dataset over the zip codes.
    The join will be of 'Inner' type because we need to keep the avg income information
    and the longitude-latitude data. This join will produce a new DataFrame. Then, we
    join the dataset DataFrame with the new DataFrame on the longitude-latitude column.
    This join will be of 'Inner' type as well to guarantee that the combination of
    <Victim Descent, Zip Code> exists. TODO<ADD DESCRIPTION FOR THE TOP AND BOTTOM 3>
    Finally, a groupBy-count operation is done to present the result.

    :return:
    """
    spark = SparkSession.builder \
        .appName("Query-3") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

    crime_dataset_filename = 'Crime_Data_all.csv'
    income_dataset_filename = 'LA_income_2015.csv'
    rev_geocoding_dataset_filename = 'revgecoding.csv'
    hdfs_path = get_hdfs_path()

    income_path = f'{hdfs_path}/{income_dataset_filename}'
    rev_gecoding_path = f'{hdfs_path}/{rev_geocoding_dataset_filename}'
    dataset_path = f'{hdfs_path}/{crime_dataset_filename}'

    income_2015_df = spark.read.csv(path=income_path, header=True, schema=income_dataset_schema)

    income_2015_df = income_2015_df.withColumn(f'{EST_MEDIAN_INCOME_COL_NAME}_NUM',
                                               regexp_replace(col(EST_MEDIAN_INCOME_COL_NAME), "[$,]", "").cast("int"))

    # TODO Parameterize the path
    rev_gecoding_df = spark.read.csv(path=rev_gecoding_path, header=True, schema=rev_geocoding_dataset_schema)
    rev_gecoding_df = rev_gecoding_df.withColumn(LON_LAT_CONCAT_COL_NAME,
                                                 concat(col(LATITUDE_COL_NAME), lit(','), col(LONGITUDE_COL_NAME)))

    print(
        f'Geocoding location has > 1 zip code? Answer: {location_has_more_than_one_zip_code(rev_gecoding_df, LON_LAT_CONCAT_COL_NAME)}')

    df = spark.read.csv(dataset_path, header=True, inferSchema=True)  # schema=main_dataset_schema)

    print(f'Income dataset size: {income_2015_df.count()}')
    print(f'Rev geocoding dataset size: {rev_gecoding_df.count()}')
    print(f'Original dataset size: {df.count()}')

    # Filter to get Year 2015
    df = df.withColumn('date_reported_timestamp', to_timestamp(df[DATE_REPORTED_COL_NAME], "MM/dd/yyyy hh:mm:ss a"))
    df = df.withColumn('Year', year(df['date_reported_timestamp']))
    df2015 = df.filter(df['Year'] == 2015)

    print(f'Year-2015 dataset size: {df2015.count()}')

    df2015 = df2015.filter(df2015[VICTIM_DESCENT_COL_NAME].isNotNull())

    print(f'Year-2015 dataset size (null victims removed): {df2015.count()}')

    df2015 = df2015.withColumn(LON_LAT_CONCAT_COL_NAME,
                               concat(col(LATITUDE_COL_NAME), lit(','), col(LONGITUDE_COL_NAME)))

    # Construct <income, lon-lat> dataset by inner join
    income_rev_geocoding_df = income_2015_df.join(rev_gecoding_df,
                                                  income_2015_df[INC_ZIP_CODE_COL_NAME] ==
                                                  rev_gecoding_df[REV_ZIP_CODE_COL_NAME],
                                                  how='inner')

    final_joined_df = df2015.join(income_rev_geocoding_df, on=LON_LAT_CONCAT_COL_NAME, how='inner')

    print(f'Triple joined DF size: {final_joined_df.count()}')

    grouped_by_df = final_joined_df.groupBy(VICTIM_DESCENT_COL_NAME, f'{EST_MEDIAN_INCOME_COL_NAME}_NUM').count()

    print('Calculating the total number of victims per descent for the highest 3 avg income locations...')
    top3_window_spec = Window.partitionBy(VICTIM_DESCENT_COL_NAME) \
        .orderBy(col(f'{EST_MEDIAN_INCOME_COL_NAME}_NUM').desc())
    top3_df = grouped_by_df.withColumn("row_num", row_number().over(top3_window_spec))
    top3_df = top3_df.filter(col("row_num") <= 3).drop("row_num")

    top3_df = top3_df.groupBy(VICTIM_DESCENT_COL_NAME) \
        .agg(sum("count").alias('Total Victims')) \
        .orderBy(col('Total Victims').desc())

    top3_df = get_full_premise_descent_category(top3_df)
    top3_df.show(100)

    print('Calculating the total number of victims per descent for the lowest 3 avg income locations...')
    bottom3_window_spec = Window.partitionBy(VICTIM_DESCENT_COL_NAME) \
        .orderBy(col(f'{EST_MEDIAN_INCOME_COL_NAME}_NUM').asc())
    bottom3_df = grouped_by_df.withColumn("row_num", row_number().over(bottom3_window_spec))
    bottom3_df = bottom3_df.filter(col("row_num") <= 3).drop("row_num")

    bottom3_df = bottom3_df.groupBy(VICTIM_DESCENT_COL_NAME) \
        .agg(sum("count").alias('Total Victims')) \
        .orderBy(col('Total Victims').desc())

    bottom3_df = get_full_premise_descent_category(bottom3_df)
    bottom3_df.show(100)

    spark.stop()


def question_d():
    """
    TODO: ADD
    :return:
    """

    def get_distance(lat1, lon1, lat2, lon2) -> float:
        import math
        from builtins import round
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

    # import sys
    # print(sys.executable)
    # import os
    # os.environ['PYSPARK_PYTHON'] = r'C:\Users\panos\anaconda3\envs\dsml-large-scale-data-management-env\python.exe'
    # os.environ[
    #     'PYSPARK_DRIVER_PYTHON'] = r'C:\Users\panos\anaconda3\envs\dsml-large-scale-data-management-env\python.exe'

    spark = SparkSession.builder \
        .appName("Query-4-DataFrame") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

    crime_dataset_filename = 'Crime_Data_all.csv'
    q4_dataset_filename = 'query-4-data.csv'
    hdfs_path = get_hdfs_path()

    dataset_path = f'{hdfs_path}/{crime_dataset_filename}'
    q4_dataset_path = f'{hdfs_path}/{q4_dataset_filename}'

    df = spark.read.csv(path=dataset_path, header=True, inferSchema=True)

    print(f'Original DataFrame size: {df.count()}')

    weapon_filtered_df = df.filter((df[WEAPON_USED_CODE_COL_NAME] >= 100) &
                                   (df[WEAPON_USED_CODE_COL_NAME] < 200))

    print(f'Dataset which has only weapon crimes: {weapon_filtered_df.count()}')

    clear_df = weapon_filtered_df.filter((weapon_filtered_df[LONGITUDE_COL_NAME] != 0) &
                                         (weapon_filtered_df[LATITUDE_COL_NAME] != 0))

    print(f'Dataset which has only weapon crimes after removing NULL Island entries: {clear_df.count()}')

    q4_df = spark.read.csv(path=q4_dataset_path, header=True, inferSchema=True)

    joined_df = clear_df.join(q4_df, q4_df[Q4_PRECINCT_COL_NAME] == clear_df['AREA'], how='inner')
    get_distance_udf = udf(get_distance, FloatType())

    DISTANCE_COL_NAME = 'distance'

    df_with_distance = joined_df.withColumn(DISTANCE_COL_NAME, get_distance_udf(LATITUDE_COL_NAME, LONGITUDE_COL_NAME,
                                                                                Q4_LATITUDE_COL_NAME,
                                                                                Q4_LONGITUDE_COL_NAME))

    TOTAL_INCIDENTS_COL_NAME = 'incidents total'
    df_with_distance.groupBy(col(Q4_DIVISION_COL_NAME)) \
        .agg(count('*').alias(TOTAL_INCIDENTS_COL_NAME),
             round(avg(DISTANCE_COL_NAME), 3).alias('AVERAGE_DISTANCE')) \
        .orderBy(col(TOTAL_INCIDENTS_COL_NAME).desc()) \
        .show(100)

    spark.stop()


def question_d_rdd():
    """
    :return:
    """
    print('Running in RDD mode...')
    spark = SparkSession.builder \
        .appName("Query-4-RDD") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

    crime_dataset_filename = 'Crime_Data_all.csv'
    q4_dataset_filename = 'query-4-data.csv'
    hdfs_path = get_hdfs_path()

    dataset_path = f'{hdfs_path}/{crime_dataset_filename}'
    q4_dataset_path = f'{hdfs_path}/{q4_dataset_filename}'

    df = spark.read.csv(path=dataset_path, header=True, inferSchema=True)
    q4_df = spark.read.csv(path=q4_dataset_path, header=True, inferSchema=True)

    crime_rdd = df.rdd
    q4_rdd = q4_df.rdd

    crime_rdd = crime_rdd.map(lambda row: (row['AREA'], (row[WEAPON_USED_CODE_COL_NAME],
                                                         row[LONGITUDE_COL_NAME],
                                                         row[LATITUDE_COL_NAME],
                                                         row)))

    q4_rdd = q4_rdd.map(lambda row: (row[Q4_PRECINCT_COL_NAME], (row[Q4_LATITUDE_COL_NAME],
                                                                 row[Q4_LONGITUDE_COL_NAME],
                                                                 row[Q4_DIVISION_COL_NAME],
                                                                 row)))
    joined_rdd = crime_rdd.join(q4_rdd)

    # row[1][0][0] is the data from WEAPON_USED_CODE_COL_NAME
    # row[1][0][1] is the data from LONGITUDE_COL_NAME
    # row[1][0][2] is the data from LATITUDE_COL_NAME
    # Filter 1xx & Null island
    filtered_rdd = joined_rdd.filter(lambda row: (row[1][0][0] is not None) and (100 <= row[1][0][0] < 200))\
                             .filter(lambda row: (row[1][0][1] != 0) and (row[1][0][2] != 0))
    # e.g: (20, 102, -118.2892, 34.0763, (34.050208529, -118.291175911, 'OLYMPIC', Row(X=-118.291175911, Y=34.050208529, FID=11, DIVISION='OLYMPIC', LOCATION='1130 S. VERMONT AVE.', PREC=20)))
    # results = filtered_rdd.map(lambda row: (row[0], row[1][0][0], row[1][0][1], row[1][0][2], row[1][1])).collect()

    def get_distance(lat1, lon1, lat2, lon2) -> float:
        import math
        from builtins import round
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

    # (7, (
    #      (102, -118.3436, 34.0534, Row(DR_NO=100700528, Date Rptd='01/08/2010 12:00:00 AM', DATE OCC='01/08/2010 12:00:00 AM', TIME OCC=1005, AREA=7, AREA NAME='Wilshire', Rpt Dist No=765, Part 1-2=1, Crm Cd=210, Crm Cd Desc='ROBBERY', Mocodes='1300 1501', Vict Age=58, Vict Sex='M', Vict Descent='H', Premis Cd=217, Premis Desc='AUTO REPAIR SHOP', Weapon Used Cd=102, Weapon Desc='HAND GUN', Status='IC', Status Desc='Invest Cont', Crm Cd 1=210, Crm Cd 2=None, Crm Cd 3=None, Crm Cd 4=None, LOCATION='1200 S  LA BREA AV', Cross Street=None, LAT=34.0534, LON=-118.3436)),
    #      (34.046747682, -118.342829525, 'WILSHIRE', Row(X=-118.342829525, Y=34.046747682, FID=10, DIVISION='WILSHIRE', LOCATION='4861 VENICE BLVD.', PREC=7))
    #      )
    # )
    clear_rdd = filtered_rdd.map(lambda row: (row[0],
                                              get_distance(row[1][0][2], row[1][0][1],
                                                           row[1][1][0], row[1][1][1]),
                                              row[1][1][2]))
    grouped_rdd = clear_rdd.groupBy(lambda x: x[2])

    def calculate_sum_and_count(values):
        from builtins import sum
        total_sum = sum(v[1] for v in values)
        total_count = len(values)
        return total_sum, total_count

    result_rdd = grouped_rdd.mapValues(calculate_sum_and_count)

    res_rdd = result_rdd.map(lambda row: (row[0], row[1][0], row[1][1]))
    sorted_rdd = res_rdd.sortBy(lambda x: x[2], ascending=False)

    schema = ['division', 'average_distance', 'incidents_total']
    df = spark.createDataFrame(sorted_rdd, schema=schema)
    df.show(100)

    spark.stop()


def get_full_premise_descent_category(df):
    df = df.withColumn('Victim_Descent_Detailed',
                       when(df[VICTIM_DESCENT_COL_NAME] == 'A', 'Other Asian')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'B', 'Black')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'C', 'Chinese')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'D', 'Cambodian')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'F', 'Filipino')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'G', 'Guamanian')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'H', 'Hispanic/Latin/Mexican')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'I', 'American Indian/Alaskan Native')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'J', 'Japanese')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'K', 'Korean')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'L', 'Laotian')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'O', 'Other')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'P', 'Pacific Islander')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'S', 'Samoan')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'U', 'Hawaiian')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'V', 'Vietnamese')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'W', 'White')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'X', 'Unknown')
                       .when(df[VICTIM_DESCENT_COL_NAME] == 'Z', 'Asian Indian')
                       .otherwise('Missing Descent')
                       )
    return df.select('Victim_Descent_Detailed', col('Total Victims'))
