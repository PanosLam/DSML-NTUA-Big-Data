from pyspark.sql.types import StructType, StructField, IntegerType, StringType


"""
Dataset column definitions
"""
# Main Dataset column names
AREA_COL_NAME = 'AREA '
PREMISE_CODE_COL_NAME = 'Premis Cd'
TIME_OCCURRED_COL_NAME = 'TIME OCC'
DATE_REPORTED_COL_NAME = 'Date Rptd'
VICTIM_DESCENT_COL_NAME = 'Vict Descent'
TIMESTAMP_FORMAT = 'MM/dd/yyyy hh:mm:ss a'
WEAPON_USED_CODE_COL_NAME = 'Weapon Used Cd'
CRIME_CODE_DESCRIPTION_COL_NAME = 'Crm Cd Desc'

# Income Dataset column names
INC_ZIP_CODE_COL_NAME = 'Zip Code'
COMMUNITY_COL_NAME = 'Community'
EST_MEDIAN_INCOME_COL_NAME = 'Estimated Median Income'

# Reverse Geo-coding Dataset column names
LATITUDE_COL_NAME = 'LAT'
LONGITUDE_COL_NAME = 'LON'
REV_ZIP_CODE_COL_NAME = 'ZIPcode'

# Query-4 Additional data column names
Q4_LONGITUDE_COL_NAME = 'X'
Q4_LATITUDE_COL_NAME = 'Y'
Q4_PRECINCT_COL_NAME = 'PREC'
Q4_DIVISION_COL_NAME = 'DIVISION'


"""
Schema Definitions
"""
main_dataset_schema = StructType([
    StructField(TIME_OCCURRED_COL_NAME, IntegerType(), True),
    StructField(DATE_REPORTED_COL_NAME, StringType(), True),
    StructField(PREMISE_CODE_COL_NAME, IntegerType(), True),
    StructField(VICTIM_DESCENT_COL_NAME, StringType(), True),
    StructField(LATITUDE_COL_NAME, StringType(), True),
    StructField(LONGITUDE_COL_NAME, StringType(), True)
])

income_dataset_schema = StructType([
    StructField(INC_ZIP_CODE_COL_NAME, IntegerType(), False),
    StructField(COMMUNITY_COL_NAME, StringType(), False),
    StructField(EST_MEDIAN_INCOME_COL_NAME, StringType(), False)
])

rev_geocoding_dataset_schema = StructType([
    StructField(LATITUDE_COL_NAME, StringType(), False),
    StructField(LONGITUDE_COL_NAME, StringType(), False),
    StructField(REV_ZIP_CODE_COL_NAME, IntegerType(), False)
])
