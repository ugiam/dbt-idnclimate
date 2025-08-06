import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

ACCOUNT = os.getenv("ACCOUNT")
USERDE = os.getenv("USERDE")
PASSDE = os.getenv("PASSDE")

conn = snowflake.connector.connect(
    user=USERDE,
    password=PASSDE,
    account=ACCOUNT,
    warehouse='COMPUTE_WH',
    database='IDNCLIMATE',
)

cur = conn.cursor()

# create schema and stage for external files
cur.execute("CREATE OR REPLACE SCHEMA EXTERNAL_STAGES;")
cur.execute("USE SCHEMA EXTERNAL_STAGES;")
cur.execute("""CREATE OR REPLACE STAGE IDNCLIMATE_STAGE
                FILE_FORMAT = (TYPE = 'CSV')
                DIRECTORY = (ENABLE = TRUE);"""
            )
cur.execute("PUT file://./indonesia-climate-daily-data/*.csv @IDNCLIMATE_STAGE;")

# create table and import data from external files (climate_data)
cur.execute("USE SCHEMA RAW;")
cur.execute("""CREATE OR REPLACE TABLE RAW_CLIMATE_DATA
                    (date datetime,
                     station_id integer,
                     min_temperature float,
                     max_temperature float,
                     avg_temperature float,
                     avg_humidity float,
                     rainfall_intensity float,
                     sunshine_shour float,
                     wind_max_speed float,
                     wind_direction varchar(5),
                     wind_avg_speed float
                     );"""
            )
cur.execute("""COPY INTO RAW_CLIMATE_DATA
                FROM (SELECT
                    TO_TIMESTAMP(i.$1::VARCHAR, 'DD-MM-YYYY'),
                    i.$12,
                    i.$2,
                    i.$3,
                    i.$4,
                    i.$5,
                    i.$6,
                    i.$7,
                    i.$8,
                    i.$11,
                    i.$10
                    FROM @IDNCLIMATE.EXTERNAL_STAGES.IDNCLIMATE_STAGE/climate_data.csv.gz i)
                FILE_FORMAT = (TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                COMPRESSION = 'GZIP');"""
            )

# create table and import data from external files (province)
cur.execute("USE SCHEMA RAW;")
cur.execute("""CREATE OR REPLACE TABLE RAW_PROVINCE
                    (id integer,
                     name string);"""
            )
cur.execute("""COPY INTO RAW_PROVINCE
                FROM @IDNCLIMATE.EXTERNAL_STAGES.IDNCLIMATE_STAGE/province_detail.csv.gz
                FILE_FORMAT = (TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                COMPRESSION = 'GZIP');"""
            )

# create table and import data from external files (region)
cur.execute("USE SCHEMA RAW;")
cur.execute("""CREATE OR REPLACE TABLE RAW_REGION
                    (id integer,
                     name string,
                     province_id integer);"""
            )
cur.execute("""COPY INTO RAW_REGION
                FROM (SELECT
                    i.$6,
                    i.$3,
                    i.$7
                    FROM @IDNCLIMATE.EXTERNAL_STAGES.IDNCLIMATE_STAGE/station_detail.csv.gz i)
                FILE_FORMAT = (TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                COMPRESSION = 'GZIP');"""
            )

# create table and import data from external files (station)
cur.execute("USE SCHEMA RAW;")
cur.execute("""CREATE OR REPLACE TABLE RAW_STATION
                    (id integer,
                     name string,
                     region_id integer,
                     lattitude float,
                     longitude float);"""
            )
cur.execute("""COPY INTO RAW_STATION
                FROM (SELECT
                    i.$1,
                    i.$2,
                    i.$6,
                    i.$4,
                    i.$5
                    FROM @IDNCLIMATE.EXTERNAL_STAGES.IDNCLIMATE_STAGE/station_detail.csv.gz i)
                FILE_FORMAT = (TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                COMPRESSION = 'GZIP');"""
            )