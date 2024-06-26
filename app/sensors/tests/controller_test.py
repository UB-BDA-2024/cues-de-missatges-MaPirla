from fastapi.testclient import TestClient
import pytest
from app.main import app
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.cassandra_client import CassandraClient
from app.elasticsearch_client import ElasticsearchClient


client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def clear_dbs():
     from shared.database import engine
     from shared.sensors import models
     models.Base.metadata.drop_all(bind=engine)
     models.Base.metadata.create_all(bind=engine)
     redis = RedisClient(host="redis")
     redis.clearAll()
     redis.close()
     mongo = MongoDBClient(host="mongodb")
     mongo.clearDb("sensors")
     mongo.close()

#
def test_create_sensor_temperatura_1():
    """A sensor can be properly created"""
    response = client.post("/sensors", json={"name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"})
    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}
#
def test_create_sensor_velocitat_1():
    response = client.post("/sensors", json={"name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"})
    assert response.status_code == 200
    assert response.json() == {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}
#
def test_create_sensor_velocitat_2():
    response = client.post("/sensors", json={"name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"})
    assert response.status_code == 200
    assert response.json() == {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}
#
def test_create_sensor_temperatura_2():
    """A sensor can be properly created"""
    response = client.post("/sensors", json={"name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"})
    assert response.status_code == 200
    assert response.json() == {"id": 4, "name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}
#
def test_post_sensor_data_temperatura_1():
    response = client.post("/sensors/1/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200
#
def test_post_sensor_data_temperatura_2():
    response = client.post("/sensors/1/data", json={"temperature": 4.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200
#
def test_post_sensor_data_temperatura_3():
    response = client.post("/sensors/4/data", json={"temperature": 15.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-02T00:00:00.000Z"})
    assert response.status_code == 200
#
def test_post_sensor_data_temperatura_4():
    response = client.post("/sensors/4/data", json={"temperature": 17.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-02T00:00:00.000Z"})
    assert response.status_code == 200

#
def test_post_sensor_data_veolicitat_1():
    response = client.post("/sensors/2/data", json={"velocity": 1.0, "battery_level": 0.1, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200
#
def test_post_sensor_data_veolicitat_2():
    response = client.post("/sensors/3/data", json={"velocity": 15.0, "battery_level": 0.15, "last_seen": "2020-01-01T01:00:00.000Z"})
    assert response.status_code == 200
#
def test_get_values_sensor_temperatura():
    response = client.get("/sensors/temperature/values")
    assert response.status_code == 200
    assert response.json() == {"sensors": [
        {"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy", "values": {"max_temperature": 4.0, "min_temperature": 1.0, "average_temperature": 2.5}}, 
        {"id": 4, "name": "Sensor Temperatura 2", "latitude": 2.0, "longitude": 2.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:03", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy", "values": {"max_temperature": 17.0, "min_temperature": 15.0, "average_temperature": 16.0}}
        ]}

def test_get_sensors_quantity():
    response = client.get("/sensors/quantity_by_type")
    assert response.status_code == 200
    assert response.json() == {"sensors": [{"type":"Temperatura", "quantity": 2}, {"type":"Velocitat", "quantity": 2}]}
#
def test_get_sensors_low_battery():
    response = client.get("/sensors/low_battery")
    assert response.status_code == 200
    assert response.json() == {"sensors": [{"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1", "battery_level": 0.1}, {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2", "battery_level": 0.15}]}
#
def test_get_sensor_1():
    """A sensor can be properly retrieved"""
    response = client.get("/sensors/1")
    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}
#
def test_get_sensor_2():
    """A sensor can be properly retrieved"""
    response = client.get("/sensors/2")
    assert response.status_code == 200
    assert response.json() == {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}
#
def test_search_sensors_temperatura():
    """Sensors can be properly searched by type"""
    response = client.get('/sensors/search?query={"type":"Temperatura"}')
    assert response.status_code == 200
    assert response.json() == [{"id":1, "name":'Sensor Temperatura 1', "latitude":1.0, "longitude":1.0, "type": 'Temperatura', "mac_address":'00:00:00:00:00:00', "description":'Sensor de temperatura model Dummy Temp del fabricant Dummy', "manufacturer":'Dummy', "firmware_version":'1.0', "model":'Dummy Temp', "serie_number":'0000 0000 0000 0000'}, 
 {"id": 4, "name":'Sensor Temperatura 2', "latitude":2.0, "longitude":2.0, "type":'Temperatura', "mac_address":'00:00:00:00:00:03', "description":'Sensor de temperatura model Dummy Temp del fabricant Dummy', "manufacturer":'Dummy', "firmware_version":'1.0', "model":'Dummy Temp', "serie_number":'0000 0000 0000 0000'}]
#
def test_search_sensors_name_similar():
    """Sensors can be properly searched by name"""
    response = client.get('/sensors/search?query={"name":"Velocidad 1"}&search_type=similar')
    assert response.status_code == 200
    assert response.json() == [{"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}]

#
def test_search_sensors_type_limit():
    """Sensors can be properly searched by type"""
    response = client.get('/sensors/search?query={"type":"Velocitat"}&size=1')
    assert response.status_code == 200
    assert len(response.json()) == 1

def test_post_sensor_data_dia_1():
    response = client.post("/sensors/1/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_dia_2():
    response = client.post("/sensors/1/data", json={"temperature": 15.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-02T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_dia_3():
    response = client.post("/sensors/1/data", json={"temperature": 18.0, "humidity": 1.0, "battery_level": 0.9, "last_seen": "2020-01-03T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_veolicitat_hora_1():
    response = client.post("/sensors/2/data", json={"velocity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_veolicitat_hora_2():
    response = client.post("/sensors/2/data", json={"velocity": 15.0, "battery_level": 1.0, "last_seen": "2020-01-01T01:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_veolicitat_hora_3():
    response = client.post("/sensors/2/data", json={"velocity": 18.0, "battery_level": 0.9, "last_seen": "2020-01-01T02:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_veolicitat_week_1():
    response = client.post("/sensors/3/data", json={"velocity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_veolicitat_week_2():
    response = client.post("/sensors/3/data", json={"velocity": 15.0, "battery_level": 1.0, "last_seen": "2020-01-08T00:00:00.000Z"})
    assert response.status_code == 200

def test_post_sensor_data_veolicitat_week_3():
    response = client.post("/sensors/3/data", json={"velocity": 18.0, "battery_level": 0.9, "last_seen": "2020-01-15T00:00:00.000Z"})
    assert response.status_code == 200

def test_get_sensor_data_1_day():
    """We can get a sensor by its id"""
    response = client.get("/sensors/1/data?from=2020-01-01T00:00:00.000Z&to=2020-01-03T00:00:00.000Z&bucket=day")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 3

def test_get_sensor_data_1_week():
    response = client.get("/sensors/1/data?from=2020-01-01T00:00:00.000Z&to=2020-01-07T00:00:00.000Z&bucket=week")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 1

def test_get_sensor_data_2_hour():
    response = client.get("/sensors/2/data?from=2020-01-01T00:00:00.000Z&to=2020-01-01T02:00:00.000Z&bucket=hour")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 3

def test_get_sensor_data_2_day():
    response = client.get("/sensors/2/data?from=2020-01-01T00:00:00.000Z&to=2020-01-02T00:00:00.000Z&bucket=day")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 1

def test_get_sensor_data_3_week():
    response = client.get("/sensors/3/data?from=2020-01-01T00:00:00.000Z&to=2020-01-15T00:00:00.000Z&bucket=week")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 3

def test_get_sensor_data_3_month():
    response = client.get("/sensors/3/data?from=2020-01-01T00:00:00.000Z&to=2020-01-31T00:00:00.000Z&bucket=month")
    assert response.status_code == 200
    json = response.json()
    assert len(json) == 1
    