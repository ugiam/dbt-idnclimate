import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

ACCOUNT = os.getenv("ACCOUNT")
USERADMIN = os.getenv("USERADMIN")
PASSADMIN = os.getenv("PASSADMIN")
USERDE = os.getenv("USERDE")
PASSDE = os.getenv("PASSDE")

conn = snowflake.connector.connect(
    user=USERADMIN,
    password=PASSADMIN,
    account=ACCOUNT,
)

cur = conn.cursor()

# create role
cur.execute("USE ROLE ACCOUNTADMIN;")
cur.execute("CREATE ROLE IF NOT EXISTS DATA_ENGINEER;")
cur.execute("GRANT ROLE DATA_ENGINEER TO ROLE ACCOUNTADMIN;")

# create warehouse
cur.execute("USE ROLE ACCOUNTADMIN;")
cur.execute("CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;")
cur.execute("GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER;")

# create user
cur.execute(f"""CREATE USER IF NOT EXISTS {USERDE}
  PASSWORD='{PASSDE}'
  LOGIN_NAME='{USERDE}'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=DATA_ENGINEER
  DEFAULT_NAMESPACE='IDNCLIMATE.RAW'
  COMMENT='{USERDE} user used for data transformation';"""
            )
cur.execute(f"ALTER USER {USERDE} SET TYPE = LEGACY_SERVICE;")
cur.execute(f"GRANT ROLE DATA_ENGINEER to USER {USERDE};")


# create database and schema
cur.execute("CREATE DATABASE IF NOT EXISTS IDNCLIMATE;")
cur.execute("CREATE SCHEMA IF NOT EXISTS IDNCLIMATE.RAW;")

# grant permission to role data_engineer
cur.execute("GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER;")
cur.execute("GRANT ALL ON DATABASE IDNCLIMATE to ROLE DATA_ENGINEER;")
cur.execute("GRANT ALL ON ALL SCHEMAS IN DATABASE IDNCLIMATE to ROLE DATA_ENGINEER;")
cur.execute("GRANT ALL ON FUTURE SCHEMAS IN DATABASE IDNCLIMATE to ROLE DATA_ENGINEER;")
cur.execute("GRANT ALL ON ALL TABLES IN SCHEMA IDNCLIMATE.RAW to ROLE DATA_ENGINEER;")
cur.execute("GRANT ALL ON FUTURE TABLES IN SCHEMA IDNCLIMATE.RAW to ROLE DATA_ENGINEER;")