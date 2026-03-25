CREATE DATABASE ev_charging;
USE ev_charging;
CREATE TABLE charging_stations_raw (
    uuid VARCHAR(50),
    operator_id INT,
    usage_type_id INT,
    usage_cost VARCHAR(50),
    name VARCHAR(255),
    address VARCHAR(255),
    town VARCHAR(100),
    state VARCHAR(100),
    postcode VARCHAR(20),
    country_id INT,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    status_type_id INT,
    date_created DATETIME
);


CREATE TABLE stations_raw (
    station_id VARCHAR(50),
    uuid VARCHAR(50),
    operator_id VARCHAR(50),
    usage_type_id VARCHAR(50),
    usage_cost VARCHAR(100),
    name VARCHAR(255),
    address VARCHAR(255),
    town VARCHAR(100),
    state VARCHAR(100),
    postcode VARCHAR(20),
    country_id VARCHAR(50),
    latitude VARCHAR(50),
    longitude VARCHAR(50),
    status_type_id VARCHAR(50),
    date_created VARCHAR(50)
);

LOAD DATA LOCAL INFILE 
'C:/Users/armaa/Documents/projects/DBMA Project/Cleaned Data CSV/charging_stations_india.csv'
INTO TABLE stations_raw
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(station_id,
 uuid,
 operator_id,
 usage_type_id,
 usage_cost,
 name,
 address,
 town,
 state,
 postcode,
 country_id,
 latitude,
 longitude,
 status_type_id,
 date_created);
-- Checking Nill postal codes  
 SELECT 
    COUNT(*) AS total,
    SUM(postcode IS NULL OR postcode = '') AS null_postcode
FROM stations_raw;

 

 SELECT COUNT(*) FROM stations_raw;
SELECT * FROM stations_raw LIMIT 5;

SELECT COUNT(DISTINCT station_id) AS unique_stations,
       COUNT(*) AS total_rows
FROM stations_raw;

SELECT COUNT(DISTINCT state) AS distinct_states
FROM stations_raw;

SELECT state, COUNT(*) 
FROM stations_raw
GROUP BY state
ORDER BY COUNT(*) DESC;

CREATE TABLE state_mapping (
    raw_state VARCHAR(100),
    standardized_state VARCHAR(100)
);


INSERT INTO state_mapping VALUES
('Mp','Madhya Pradesh'),
('Mh','Maharashtra'),
('Gj','Gujarat'),
('Ka','Karnataka'),
('Rj','Rajasthan'),
('Karnatak','Karnataka'),
('Keral','Kerala'),
('Keraka','Kerala'),
('Lerala','Kerala'),
('Mahrashtra','Maharashtra'),
('Tamilnadu','Tamil Nadu'),
('Tamil Nasdu','Tamil Nadu'),
('Uttarakhnad','Uttarakhand'),
('New Dehi','Delhi'),
('Bangalore Urban','Karnataka'),
('Chennai','Tamil Nadu'),
('Villupuram','Tamil Nadu'),
('India', NULL);

DESCRIBE stations_raw;

ALTER TABLE stations_raw
ADD COLUMN state_clean VARCHAR(100);

UPDATE stations_raw
SET state_clean = state;

UPDATE stations_raw s
JOIN state_mapping m
ON s.state_clean = m.wrong_state
SET s.state_clean = m.correct_state;


UPDATE stations_raw s
JOIN state_mapping m
ON s.state_clean = m.raw_state
SET s.state_clean = m.standardized_state;

SELECT state_clean, COUNT(*)
FROM stations_raw
GROUP BY state_clean
ORDER BY COUNT(*) DESC;

UPDATE stations_raw
SET state_clean = NULL
WHERE state_clean = 'None';

UPDATE stations_raw
SET state_clean = NULL
WHERE TRIM(state_clean) = '';

SELECT 
    COUNT(*) AS total_rows,
    COUNT(state_clean) AS non_null_states,
    COUNT(*) - COUNT(state_clean) AS null_states
FROM stations_raw;

ALTER TABLE stations_raw
ADD COLUMN usage_cost_numeric DECIMAL(10,2);

UPDATE stations_raw
SET usage_cost_numeric =
CAST(
    REGEXP_SUBSTR(usage_cost, '[0-9]+(\\.[0-9]+)?')
AS DECIMAL(10,2));

SELECT usage_cost, usage_cost_numeric
FROM stations_raw
LIMIT 20;


SELECT COUNT(*)
FROM stations_raw
WHERE usage_cost_numeric IS NULL;

-- Splitting into relational tables

CREATE TABLE stations (
    station_id INT PRIMARY KEY,
    uuid VARCHAR(100),
    operator_id INT,
    name VARCHAR(255),
    address VARCHAR(255),
    town VARCHAR(100),
    state_clean VARCHAR(100),
    postcode VARCHAR(20),
    country_id INT,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    status_type_id INT,
    date_created DATETIME
);

SELECT station_id, COUNT(*)
FROM stations_raw
GROUP BY station_id
HAVING COUNT(*) > 1;


INSERT INTO stations (
    station_id,
    uuid,
    operator_id,
    name,
    address,
    town,
    state,
    postcode,
    country_id,
    latitude,
    longitude,
    status_type_id,
    date_created
);

DESCRIBE stations;

INSERT INTO stations (
    station_id,
    uuid,
    operator_id,
    name,
    address,
    town,
    state_clean,
    postcode,
    country_id,
    latitude,
    longitude,
    status_type_id,
    date_created
)
SELECT
    station_id,
    uuid,
    NULLIF(operator_id, ''),
    name,
    address,
    town,
    state_clean,
    postcode,
    NULLIF(country_id, ''),
    NULLIF(latitude, ''),
    NULLIF(longitude, ''),
    NULLIF(status_type_id, ''),
    STR_TO_DATE(
        REPLACE(REPLACE(date_created, 'T', ' '), 'Z', ''),
        '%Y-%m-%d %H:%i:%s'
    )
FROM stations_raw;

SELECT station_id, latitude, longitude
FROM stations
LIMIT 10;

-- checking of any coordinates become null
SELECT COUNT(*)
FROM stations
WHERE latitude IS NULL OR longitude IS NULL;

-- Creating Tables from Units Database 

CREATE TABLE units_raw (
    connection_id VARCHAR(50),
    station_id VARCHAR(50),
    connection_type_id VARCHAR(50),
    level_id VARCHAR(50),
    power_kw VARCHAR(50),
    current_type_id VARCHAR(50),
    quantity VARCHAR(50),
    status_type_id VARCHAR(50)
);

-- Loading the dataset from local storage 

LOAD DATA LOCAL INFILE 
'C:/Users/armaa/Documents/projects/DBMA Project/Cleaned Data CSV/charging_units_india.csv'
INTO TABLE units_raw
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- DATA Audit

SELECT COUNT(*) FROM units_raw;

SELECT 
    COUNT(*) total,
    COUNT(connection_id) conn_id_not_null,
    COUNT(station_id) station_id_not_null
FROM units_raw;

SELECT DISTINCT quantity FROM units_raw LIMIT 10;
SELECT DISTINCT level_id FROM units_raw LIMIT 10;

-- Creating Unites Table 
CREATE TABLE units (
    connection_id INT PRIMARY KEY,
    station_id INT,
    connection_type_id INT,
    level_id INT,
    power_kw DECIMAL(10,2),
    current_type_id INT,
    quantity INT,
    status_type_id INT,
    
    FOREIGN KEY (station_id) REFERENCES stations(station_id)
);
-- Clean Data Strip 

INSERT INTO units (
    connection_id,
    station_id,
    connection_type_id,
    level_id,
    power_kw,
    current_type_id,
    quantity,
    status_type_id
)
SELECT
    CAST(connection_id AS UNSIGNED),
    CAST(station_id AS UNSIGNED),
    CAST(connection_type_id AS UNSIGNED),
    CAST(level_id AS UNSIGNED),
    CAST(power_kw AS DECIMAL(10,2)),
    CAST(current_type_id AS UNSIGNED),
    CAST(quantity AS UNSIGNED),
    CAST(status_type_id AS UNSIGNED)
FROM units_raw;


INSERT INTO units (
    connection_id,
    station_id,
    connection_type_id,
    level_id,
    power_kw,
    current_type_id,
    quantity,
    status_type_id
)
SELECT
    CAST(connection_id AS UNSIGNED),
    CAST(station_id AS UNSIGNED),
    CAST(connection_type_id AS UNSIGNED),

    CAST(CAST(NULLIF(level_id, '') AS DECIMAL(10,2)) AS UNSIGNED),

    CAST(NULLIF(power_kw, '') AS DECIMAL(10,2)),

    CAST(CAST(NULLIF(current_type_id, '') AS DECIMAL(10,2)) AS UNSIGNED),

    CAST(CAST(NULLIF(quantity, '') AS DECIMAL(10,2)) AS UNSIGNED),

    CAST(CAST(NULLIF(status_type_id, '') AS DECIMAL(10,2)) AS UNSIGNED)

FROM units_raw;


SELECT *
FROM units_raw
WHERE level_id = ''
   OR current_type_id = ''
   OR quantity = ''
   OR status_type_id = ''
   OR power_kw = '';

SELECT COUNT(*) FROM units;

-- Creating Tables 

CREATE TABLE operators (
    operator_id INT PRIMARY KEY
);

CREATE TABLE countries (
    country_id INT PRIMARY KEY
);


CREATE TABLE status_types (
    status_type_id INT PRIMARY KEY
);

INSERT INTO operators
SELECT DISTINCT operator_id
FROM stations
WHERE operator_id IS NOT NULL;

INSERT INTO countries
SELECT DISTINCT country_id
FROM stations
WHERE country_id IS NOT NULL;

INSERT INTO connection_types
SELECT DISTINCT connection_type_id
FROM units;

INSERT INTO status_types
SELECT DISTINCT status_type_id FROM stations
UNION
SELECT DISTINCT status_type_id FROM units;


-- Add foreign keys( units -> Stations)
ALTER TABLE units
ADD CONSTRAINT fk_units_station
FOREIGN KEY (station_id)
REFERENCES stations(station_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- Operators Foreign Keys 
 
SELECT COUNT(*) FROM stations
WHERE operator_id IS NOT NULL
AND operator_id NOT IN (
    SELECT operator_id FROM operators
);

SELECT COUNT(*) FROM stations
WHERE country_id IS NOT NULL
AND country_id NOT IN (
    SELECT country_id FROM countries
);

-- ADD Foriegn Keys Stations -> units 
ALTER TABLE units
ADD CONSTRAINT fk_units_station
FOREIGN KEY (station_id)
REFERENCES stations(station_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

SHOW CREATE TABLE units;
ALTER TABLE units
DROP FOREIGN KEY units_ibfk_1;

SHOW CREATE TABLE stations;

-- Adding Fk stations -> operators
ALTER TABLE stations
ADD CONSTRAINT fk_stations_operator
FOREIGN KEY (operator_id)
REFERENCES operators(operator_id)
ON DELETE SET NULL
ON UPDATE CASCADE;
 
 -- Adding FK stations -> countries 
 
 ALTER TABLE stations
ADD CONSTRAINT fk_stations_country
FOREIGN KEY (country_id)
REFERENCES countries(country_id)
ON DELETE SET NULL
ON UPDATE CASCADE;

-- Add FK -> station station types

SELECT DISTINCT status_type_id
FROM stations
WHERE status_type_id IS NOT NULL
AND status_type_id NOT IN (
    SELECT status_type_id FROM status_types
);

ALTER TABLE units
ADD CONSTRAINT fk_units_status
FOREIGN KEY (status_type_id)
REFERENCES status_types(status_type_id)
ON DELETE SET NULL
ON UPDATE CASCADE;


CREATE TABLE level_types (
    level_id INT PRIMARY KEY
);

INSERT INTO level_types
SELECT DISTINCT level_id
FROM units
WHERE level_id IS NOT NULL;

SELECT COUNT(*)
FROM units
WHERE level_id IS NOT NULL
AND level_id NOT IN (SELECT level_id FROM level_types);

ALTER TABLE units
ADD CONSTRAINT fk_units_level
FOREIGN KEY (level_id)
REFERENCES level_types(level_id)
ON DELETE SET NULL
ON UPDATE CASCADE;













