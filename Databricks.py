%python

pip install snowflake-connector-python==2.5.1
pip install "snowflake-connector-python[pandas]"

"""
 This process can be in a separate notebook to designate for worker cluster.
"""
def process_file(filename, colnames):
  file_location = "/FileStore/tables/" + filename
  file_type = "csv"
  infer_schema = "false"
  first_row_is_header = "true"
  delimiter = ","
  
  # The applied options are for CSV files. For other file types, these will be ignored.
  sparkDF = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
  df= sparkDF.toPandas()
  df.columns = colnames
  return df


import snowflake.connector

"""
 1. Load data as S3 Objects.
 2. Using Access Key ID / Secreat_Access_Key..download data access from S3 buckets...
 3. Access Key ID and Secreat Access Key can be stored in the environment path and retrieved from there or from an secured INI config file.
                   os.getenv('AWS_ACCESS_KEY_ID')
                   os.getenv('AWS_SECRET_ACCESS_KEY')
 4. Snowflake warehouse credential could have been stored in AWS Secreat Manager and read from there.

"""

con = snowflake.connector.connect(
    user='anbalagan',
    password='Success#1',
    account='uda15551.us-east-1',
    warehouse='INTERVIEW_WH',
    database='USER_ANBALAGAN',
    schema='NORTHWOODS_TABLES'
    
)


import pandas
from snowflake.connector.pandas_tools import write_pandas

"""
Bulk Copy
success, nchunks, nrows, _ = write_pandas(con, df, 'NW_AIRLINE', on_error = CONTINUE )
To view all errors in the data files, use the VALIDATION_MODE parameter or query the VALIDATE function.
ROWS_PARSED  and ROWS_LOADED will provide the differnce
"""
success, nchunks, nrows, _ = write_pandas(con, process_file('airlines.csv', ['AR_CODE','AR_NAME']), 'AIRLINE' )

success, nchunks, nrows, _ = write_pandas(con, process_file('airports.csv', ['AIRPORT_CODE','AIRPORT_NAME','CITY','STATE','COUNTRY','LATITUDE','LONGITUDE']), 'AIRPORT' )

filenames = ('partition_01','partition_02','partition_03','partition_04','partition_05','partition_06','partition_07','partition_08')
colnames = ('DT_YEAR','DT_MNTH','DT_DAY','DT_DAY_OF_WEEK','AR_CODE','FLIGHT_NUMBER','TAIL_NUMBER'
                                  'ORIGIN_AIRPORT','DESTINATION_AIRPORT','SCHEDULED_DEPARTURE','DEPARTURE_TIME',
                                   'DEPARTURE_DELAY','TAXI_OUT','WHEELS_OFF','SCHEDULED_TIME','ELAPSED_TIME','AIR_TIME','DISTANCE',
                                   'WHEELS_ON','TAXI_IN','SCHEDULED_ARRIVAL','ARRIVAL_TIME','ARRIVAL_DELAY','DIVERTED','CANCELLED',
                                    'CANCELLATION_REASON','AIR_SYSTEM_DELAY','SECURITY_DELAY','AIRLINE_DELAY',
                                   'LATE_AIRCRAFT_DELAY','WEATHER_DELAY')

# With the support of worker cluster, this can be implemented as multiprocessing code.
for filename in filenames:
   df = process_file(filename.csv,colnames)
   df1 = df[['DT_YEAR','DT_MNTH','DT_DAY','DT_DAY_OF_WEEK']]
   success, nchunks, nrows, _ = write_pandas(con, df1, 'DATE_PART_STG')
   success, nchunks, nrows, _ = write_pandas(con, df, 'AIR_TRAFFIC_STG')
  
  """
   1. Date from the fact file , "Partition" was moved as a separate diamension table. This way we do not need to repeat all 4 fields, instead
   we can just have only one surrogate key field in fact table.
   2. Either through Stored procedure in Snowflake or direct SQL query from Databricks will be used    to remove the duplicate date records from DAET_PART_STG
   3. Move the records from DAET_PART_STG to DAET_PART and trucncate DAET_PART_STG to free-up the strogage cost.
   4. AIR_TRAFFIC will not have the repetitive date related fields. aMove the records from AIR_TRAFFIC_STG to AIR_TRAFFIC by assoicating the Surrogage Key in DATE_PART.
   """
  
   
   






