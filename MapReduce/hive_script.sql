DROP TABLE IF EXISTS delay_flights;

CREATE EXTERNAL TABLE delay_flights ( rowID bigint, Year int, Month int, DayofMonth int, DayOfWeek int, DepTime double, CRSDepTime int, ArrTime double, CRSArrTime int, UniqueCarrier string, FlightNum int, TailNum String, ActualElapsedTime double, CRSElapsedTime double, AirTime double, ArrDelay double, DepDelay double, Origin String, Dest String, Distance int, TaxiIn double, TaxiOut double, Cancelled int, CancellationCode String, Diverted int, CarrierDelay double, WeatherDelay double, NASDelay double, SecurityDelay double, LateAircraftDelay double ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION "s3://wimalasena-23-uom-cse-bda-data/delay_flights";


INSERT OVERWRITE DIRECTORY "s3://wimalasena-23-uom-cse-bda-data/outputs/hive/q4"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
SELECT Year, SUM(CarrierDelay) AS carrier_delay, 
SUM(NASDelay) AS nas_delay,
SUM(WeatherDelay) AS weather_delay,
SUM(LateAircraftDelay) AS late_aircraft_delay,
SUM(SecurityDelay) AS security_delay
FROM delay_flights
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Compute year wise carrier delay from 2003-2010
INSERT OVERWRITE DIRECTORY "s3://wimalasena-23-uom-cse-bda-data/outputs/hive/CarrierDelay"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
SELECT Year, SUM(CarrierDelay) AS carrier_delay
FROM delay_flights
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Compute year wise NAS delay from 2003-2010
INSERT OVERWRITE DIRECTORY "s3://wimalasena-23-uom-cse-bda-data/outputs/hive/NASDelay"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
SELECT Year, SUM(NASDelay) AS nas_delay
FROM delay_flights
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Compute year wise Weather delay from 2003-2010
INSERT OVERWRITE DIRECTORY "s3://wimalasena-23-uom-cse-bda-data/outputs/hive/WeatherDelay"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
SELECT Year, SUM(WeatherDelay) AS weather_delay
FROM delay_flights
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Compute year wise late aircraft delay from 2003-2010
INSERT OVERWRITE DIRECTORY "s3://wimalasena-23-uom-cse-bda-data/outputs/hive/LateAircraftDelay"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
SELECT Year, SUM(LateAircraftDelay) AS late_aircraft_delay
FROM delay_flights
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Compute year wise security delay from 2003-2010
INSERT OVERWRITE DIRECTORY "s3://wimalasena-23-uom-cse-bda-data/outputs/hive/SecurityDelay"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
SELECT Year, SUM(SecurityDelay) AS security_delay
FROM delay_flights
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;
