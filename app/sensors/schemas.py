from pydantic import BaseModel

class MongoSensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    type: str
    mac_address: str
    description: str
    manufacturer: str
    firmware_version: str
    model: str
    serie_number: str

class SensorWithData(MongoSensor, BaseModel):
    pass


class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str
    description: str

class RedisSensorData(BaseModel):
    velocity: float | None
    temperature: float | None
    humidity: float | None
    battery_level: float
    last_seen: str

class ReturnTemperatures:
    sensors: list