import requests
import json
import sqlite3 as sq

with sq.connect("demosat_fails.db") as con:
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS fails (
                launch_id TEXT PRIMARY KEY,
                mission_name TEXT,
                success BOOLEAN,
                date DATE,
                failure_reason TEXT,
                failure_time INTEGER,
                failure_altitude INTEGER
                )""")


url = 'https://api.spacexdata.com/v4/launches'
response = requests.get(url)
if(response.status_code == 200):    
    launch_data = response.json()
    DemoSat_data = [launch for launch in launch_data if launch['name'] == 'DemoSat']
    for launch in DemoSat_data:
            launch_id = launch['id']
            mission_name = launch['name']
            success = launch['success']
            date = launch['date_utc']

            failure_reason = None
            failure_time = None
            failure_altitude = None

            if not success:
                failure_details = launch['failures'][0]
                failure_reason = failure_details['reason']
                failure_time = failure_details['time']
                failure_altitude = failure_details.get('altitude')

            cur.execute("""
            INSERT OR REPLACE INTO fails (launch_id, mission_name, success, date, failure_reason, failure_time, failure_altitude)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (launch_id, mission_name, success, date, failure_reason, failure_time, failure_altitude))
        
    con.commit()
    
    print("Data inserted successfully!")
    cur.execute('SELECT * FROM fails')
    results = cur.fetchall()
    print(results)
else:
    print("No data")


