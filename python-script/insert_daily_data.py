import snowflake.connector
import pandas as pd
from datetime import datetime
import sys
from dotenv import load_dotenv
import os

load_dotenv()

ACCOUNT = os.getenv("ACCOUNT")
USERDE = os.getenv("USERDE")
PASSDE = os.getenv("PASSDE")

today = sys.argv[1]

conn = snowflake.connector.connect(
    user=USERDE,
    password=PASSDE,
    account=ACCOUNT,
    warehouse='COMPUTE_WH',
    database='IDNCLIMATE',
)

cur = conn.cursor()
today = sys.argv[1]

df = pd.read_csv("indonesia-climate-daily-data/climate_data2.csv")
df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
df = df[df['date'] == today]

for i, doc in df.iterrows():
    try:
        if type(doc['ddd_car']) == str:
            doc['ddd_car'] = doc['ddd_car'].replace(" ", "")
        query = f"""INSERT INTO IDNCLIMATE.RAW.RAW_CLIMATE_DATA 
                    VALUES ('{doc['date'].date()}', 
                            {doc['station_id']}, 
                            {doc['Tn']}, 
                            {doc['Tx']}, 
                            {doc['Tavg']}, 
                            {doc['RH_avg']}, 
                            {doc['RR']}, 
                            {doc['ss']}, 
                            {doc['ff_x']}, 
                            '{doc['ddd_car']}', 
                            {doc['ff_avg']});
                """
        query = query.replace("nan", "null")
        # print(query)
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
        print(doc)