from cassandra.cluster import Cluster
import time

class CassandraClient:
    def __init__(self, hosts):
        print("CONNECTING")
        connected = False
        while not connected:
            try:
                self.cluster = Cluster(hosts,protocol_version=4)
                self.session = self.cluster.connect()
                connected = True
            except Exception as e:
                time.sleep(1)
        
        keyspace_query = """CREATE KEYSPACE IF NOT EXISTS sensor
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};
            """
        
        self.session.execute(keyspace_query)

        create_table_query = """
            
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id  UUID PRIMARY KEY,
                sensor_id INT,
                timestamp TIMESTAMP,
                temperature FLOAT,
                humidity FLOAT,
                velocity FLOAT,
            );
        """
        self.session.set_keyspace('sensor')
        self.session.execute(create_table_query)
        create_table_query = """
            CREATE TABLE IF NOT EXISTS sensors (
                sensor_id INT,
                name TEXT,
                type TEXT,
                PRIMARY KEY (type, sensor_id)
            );
        """
        self.session.execute(create_table_query)

    def insert_sensor(self, sensor_id: int, name: str, type: str):
        insert_query = f"""
            INSERT INTO sensors (sensor_id, type, name)
            VALUES ({sensor_id}, '{type}', '{name}');
        """
        self.session.execute(insert_query)

    def insert_entry(self, sensor_id: int, timestamp, temperature, humidity, velocity):
        temperature_or = "NULL"
        if temperature: temperature_or = temperature
        humidity_or = "NULL"
        if humidity: humidity_or = humidity
        velocity_or = "NULL"
        if velocity: velocity_or = velocity
        insert_query = f"""
            INSERT INTO sensor_readings (id, sensor_id, timestamp, temperature, humidity, velocity)
            VALUES (uuid(), {sensor_id}, '{timestamp}', {temperature_or}, {humidity_or}, {velocity_or});
        """
        self.session.execute(insert_query)


    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)
