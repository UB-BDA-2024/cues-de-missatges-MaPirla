import psycopg2
import os
from datetime import datetime

class Timescale:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
    def getCursor(self):
            return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def ping(self):
        return self.conn.ping()
    
    def execute(self, query):
       return self.cursor.execute(query)
    
    def commit(self):
        return self.conn.commit()

    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()

    def insert_data(self, sensor: int, time: str ,velocity: float | None, temperature: float | None, humidity: float | None):
        
        temperature_insert = "NULL"
        if temperature: temperature_insert = temperature
        velocity_insert = "NULL"
        if velocity: velocity_insert = velocity
        humidity_insert = "NULL"
        if humidity: humidity_insert = humidity

        query = f"""INSERT INTO sensor_data (time, sensor_id, temperature, humidity, velocity)
            VALUES ('{time}', {sensor}, {temperature_insert}, {humidity_insert}, {velocity_insert})
            """
        self.cursor.execute(query)
        self.conn.commit()
     
    def get_elements_in_interval(self, from_: datetime, to: datetime, sensor_id: int, bucket: str):
        query = f"""SELECT time_bucket('1 {bucket}', time) as bucket,
            sensor_id,
            AVG(temperature) as avg_temperature,
            AVG(humidity) as avg_humidity,
            AVG(velocity) as avg_velocity
        FROM sensor_data
        WHERE sensor_id = {sensor_id}
        AND time BETWEEN '{from_}' AND '{to}'
        GROUP BY bucket, sensor_id
        ORDER BY bucket;"""
        self.cursor.execute(query)
        return self.cursor.fetchall()
