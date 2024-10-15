import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, Date

url = 'https://api.spacexdata.com/v4/launches'
response = requests.get(url)

if response.status_code == 200:
    launch_data = response.json()

    DemoSat_data = [launch for launch in launch_data if launch['name'] == 'DemoSat']

    failed_launches = []
    for launch in DemoSat_data:
        if not launch['success']:  
            failure_details = launch['failures'][0] if launch['failures'] else {}
            failed_launches.append({
                'launch_id': launch['id'],
                'mission_name': launch['name'],
                'date': launch['date_utc'],
                'failure_reason': failure_details.get('reason', None),
                'failure_time': failure_details.get('time', None),
                'failure_altitude': failure_details.get('altitude', None)
            })

    df = pd.DataFrame(failed_launches)
else:
    print("Не вдалося отримати дані з API")
    exit()

engine = create_engine('postgresql://postgres:mysecretpassword@172.17.0.2/demosat_fails')

meta = MetaData()

fails_table = Table(
    'fails', meta,
    Column('launch_id', String, primary_key=True),
    Column('mission_name', String),
    Column('date', String),  
    Column('failure_reason', String),
    Column('failure_time', Integer),
    Column('failure_altitude', Float)
)

meta.create_all(engine)

df.to_sql('fails', con=engine, if_exists='replace', index=False)

from sqlalchemy import text

with engine.connect() as connection:
    result = connection.execute(text("SELECT * FROM fails"))
    for row in result:
        print(row)
