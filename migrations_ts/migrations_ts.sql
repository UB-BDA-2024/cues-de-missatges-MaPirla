-- #TODO: Create new TS hypertable
CREATE TABLE sensors (
    id SERIAL PRIMARY KEY,
    sensor_type VARCHAR(50)
);

CREATE TABLE sensor_data (
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    sensor_id INTEGER NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    FOREIGN KEY (sensor_id) REFERENCES sensors (id)
);

SELECT create_hypertable('sensor_data', by_range('time'));