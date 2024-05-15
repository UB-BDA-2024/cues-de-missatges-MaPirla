from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.cassandra_client import CassandraClient
import json

from app.mongodb_client import MongoDBClient
from app.redis_client import RedisClient
from app.elasticsearch_client import ElasticsearchClient
from app.timescale import Timescale

from . import models, schemas
from app import sensors

def get_sensor(db: Session, sensor_id: int) -> models.Sensor | None:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_mongodb(mongodb: MongoDBClient, sensor_id: int) -> schemas.MongoSensor | None:
    mongodb.getDatabase("MongoDB_")
    sensor_data = mongodb.getCollection("sensor").find_one({"id": sensor_id})
    return schemas.MongoSensor(
        id = sensor_data["id"],
        name = sensor_data["name"],
        manufacturer = sensor_data["manufacturer"],
        type = sensor_data["type"],
        mac_address = sensor_data["mac_address"],
        description = sensor_data["description"],
        model = sensor_data["model"],
        serie_number = sensor_data["serie_number"],
        firmware_version = sensor_data["firmware_version"],
        latitude = sensor_data["location"]["coordinates"][1],
        longitude = sensor_data["location"]["coordinates"][0]
    )

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(sensor: schemas.SensorCreate, db: Session,mongodb: MongoDBClient, elasticsearch: ElasticsearchClient, ts: Timescale, cassandra: CassandraClient) -> models.Sensor:
    
    db_sensor = create_postgres_sensor(db =db, sensor=sensor)
    mongo_sensor = create_mongo_sensor(mongodb=mongodb, sensor_create=sensor, db_sensor=db_sensor)
    create_timescale_sensor(ts=ts, sensor_create=sensor, db_sensor=db_sensor)
    cassandra.insert_sensor(db_sensor.id, sensor.name, sensor.type)
    es_sensor = {
        "id": db_sensor.id,
        "name": sensor.name,
        "type": sensor.type,
        "description": sensor.description,
    }
    elasticsearch.index_document("search_index", es_sensor)

    sensor_dict = sensor.dict()
    sensor_dict.update({"id": db_sensor.id})
    return sensor_dict

def create_postgres_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(
        name=sensor.name
        )
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

def create_mongo_sensor(mongodb: MongoDBClient, sensor_create: schemas.SensorCreate, db_sensor: models.Sensor) -> schemas.MongoSensor:
    sensor_json = {
        "id": db_sensor.id,
        "name": sensor_create.name,
        "manufacturer": sensor_create.manufacturer,
        "type": sensor_create.type,
        "mac_address": sensor_create.mac_address,
        "description": sensor_create.description,
        "model": sensor_create.model,
        "serie_number": sensor_create.serie_number,
        "firmware_version": sensor_create.firmware_version,
        "location": {
            "type": "Point",
            "coordinates": [sensor_create.longitude, sensor_create.latitude]
        }
    }
    mongodb.getDatabase("MongoDB_")
    mongodb.getCollection("sensor").insert_one(sensor_json)
    return schemas.MongoSensor(
        id = sensor_json["id"],
        name = sensor_json["name"],
        manufacturer = sensor_json["manufacturer"],
        type = sensor_json["type"],
        mac_address = sensor_json["mac_address"],
        description = sensor_json["description"],
        model = sensor_json["model"],
        serie_number = sensor_json["serie_number"],
        firmware_version = sensor_json["firmware_version"],
        latitude = sensor_json["location"]["coordinates"][1],
        longitude = sensor_json["location"]["coordinates"][0]
    )

def create_timescale_sensor(ts: Timescale, sensor_create: schemas.SensorCreate, db_sensor: models.Sensor):
    
    query_insert_sensor = f"INSERT INTO sensors (id, sensor_type) VALUES ({db_sensor.id}, '{sensor_create.type}')"
    ts.execute(query_insert_sensor)
    ts.commit()

def record_data(redis: Session, db: Session, sensor_id: int, data: schemas.RedisSensorData, ts: Timescale, mongodb: MongoDBClient, cassandra: CassandraClient) -> schemas.RedisSensorData:
    print(data)
    if get_sensor(db, sensor_id) == None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    last_seen: str = redis.get(str(sensor_id)+"_last_seen")
    if last_seen != None:
        if data.last_seen > str(last_seen):
            raise HTTPException(status_code=404, detail="Sensor not found")

    ts.insert_data(sensor_id, data.last_seen, data.velocity, data.temperature, data.humidity)
    cassandra.insert_entry(sensor_id, data.last_seen, data.temperature, data.humidity, data.velocity)
    for e in data:
        if e[1] != None:
            redis.set(str(sensor_id)+"_"+e[0], e[1])
    print(data)
    return data

def get_data(redis: Session, db: Session, mongodb: MongoDBClient, sensor_id: int) -> schemas.RedisSensorData | None:
    db_data = get_sensor(db, sensor_id)
    if db_data is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    if redis.get(str(sensor_id)+"_last_seen"):
        sensor = schemas.RedisSensorData(
            last_seen = redis.get(str(sensor_id)+"_last_seen"),
            battery_level = redis.get(str(sensor_id)+"_battery_level"),
            temperature= redis.get(str(sensor_id)+"_temperature"),
            humidity=redis.get(str(sensor_id)+"_humidity"),
            velocity=redis.get(str(sensor_id)+"_velocity")
        )
        return sensor
    else:
        return None

def delete_sensor(db: Session,redis:RedisClient, mongodb:MongoDBClient, es: ElasticsearchClient, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    mongodb.getDatabase('MongoDB_')
    collection = mongodb.getCollection('sensor')
    collection.delete_one({"id_sensor": sensor_id})
    # delete from posgreSQL
    db.delete(db_sensor)
    db.commit()
    es.client.delete_by_query("search_index",query={"query": {"match_all": {}}})
    return db_sensor

def get_sensors_near(mongodb: MongoDBClient, db: Session, redis:RedisClient, latitude: float, longitude: float, radius: int):
    mongodb.getDatabase("MongoDB_")
    collection = mongodb.getCollection("sensor")
    collection.create_index([("location", "2dsphere")])

    query = {
        "location": {
            "$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": [longitude, latitude]},
                "$maxDistance": radius  
            }
        }
    }

    near_sensors = list(collection.find(query))
    sensors = []
    for sensor in near_sensors:
        sensors.append(get_data(db=db,redis=redis, sensor_id=sensor["id_sensor"]))
    return sensors

def search_sensors(db: Session,mongodb: MongoDBClient, elasticsearch: ElasticsearchClient, query: str, size: int, search_type: str):
    print(type(query))
    query2 = json.loads(query)
    criteria = list(query2.keys())[0]
    value = query2[criteria]
    queryd = {}
    print(elasticsearch.client.indices.get_mapping(index="search_index"))
    match search_type:
        case "match":
            queryd = {
                "query": {
                    search_type: {criteria:value}
                },
                "size": size
            }
        case "prefix":
            queryd = {
                "query": {
                    search_type: {criteria+".keyword":value}
                },
                "size": size
            }
        case "similar":
            queryd = {
                "query": {
                    "match": {
                        criteria: {
                            "query": value,
                            "fuzziness": "auto",
                            "operator": "and"
                        }
                    }
                },
                "size": size
            }
        case _:
            raise HTTPException(status_code=403, detail="search type not valid")

    print(queryd)
    result = elasticsearch.search("search_index", query=queryd)
    sensors = []
    print(result, "\n\n")
    for sensor in result["hits"]["hits"]:
        sensors.append(get_sensor_mongodb(mongodb=mongodb, sensor_id=sensor["_source"]["id"]))
    print(sensors, "\n\n")
    return sensors

def get_temperature_values(db: Session, cassandra: CassandraClient, mongodb: MongoDBClient):
    sensors_list = []
    sensors = get_sensors(db)
    for sensor in sensors:
        sensor_info = get_sensor_mongodb(mongodb, sensor.id)
        if sensor_info.type == "Temperatura":
            sensor_dict = dict(sensor_info)
            
            query_temp = f"""
                SELECT
                    MIN(temperature) AS min_temperature,
                    MAX(temperature) AS max_temperature,
                    AVG(temperature) AS average_temperature
                FROM sensor_readings
                WHERE sensor_id = {sensor.id}
                ALLOW FILTERING;"""
            data = cassandra.execute(query_temp).one()
            dict_data = {
                "min_temperature": data.min_temperature,
                "max_temperature": data.max_temperature,
                "average_temperature": data.average_temperature,
            }
            sensor_dict["values"] = dict_data
            print(sensor_dict["values"])
            sensors_list.append(sensor_dict)
    return_schema = schemas.ReturnTemperatures()
    return_schema.sensors = sensors_list
    print(sensors_list)
    return  return_schema

def get_sensors_quantity(db: Session, cassandra: CassandraClient):
    query="""SELECT type, COUNT(*) FROM sensors
        GROUP BY type;
        """
    result = cassandra.execute(query)
    return [dict(row) for row in result]

def get_low_battery(db: Session, redis: RedisClient, mongodb: MongoDBClient):
    low_battery_sensors = []
    for sensor in get_sensors(db):
        bat_level = float(redis.get(str(sensor.id)+"_battery_level"))
        if bat_level < 0.2:
            sensor_info = dict(get_sensor_mongodb(mongodb, sensor.id))
            sensor_info["battery_level"] = bat_level
            low_battery_sensors.append(sensor_info)
    result = schemas.ReturnTemperatures()
    result.sensors = low_battery_sensors
    print(low_battery_sensors)
    return result