-- Todo: Add REGISTER file:/home/hadoop/lib/pig/piggybank.jar;
REGISTER s3://cs6240-data/jar/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- set 10 reduce tasks
SET default_parallel 10;

data1 = LOAD '$INPUT' USING CSVLoader();
data2 = LOAD '$INPUT' USING CSVLoader();
flight1 = FOREACH data1 GENERATE $0 AS year:int,
                                 $2 AS month:int,
                                 $5 AS flightDate:chararray,
                                 $11 AS origin:chararray,
                                 $17 AS dest:chararray,
                                 $24 AS deptTime:int,
                                 $35 AS arrTime:int,
                                 $37 AS arrDelayMin:int,
                                 $41 AS cancelled:int,
                                 $43 AS diverted:int;
flight2 = FOREACH data2 GENERATE $0 AS year:int,
                                 $2 AS month:int,
                                 $5 AS flightDate:chararray,
                                 $11 AS origin:chararray,
                                 $17 AS dest:chararray,
                                 $24 AS deptTime:int,
                                 $35 AS arrTime:int,
                                 $37 AS arrDelayMin:int,
                                 $41 AS cancelled:int,
                                 $43 AS diverted:int;
-- filter out cancelled and diverted flights and flights with orgins/destinatinos not fitting the description
flight1 = FILTER flight1 BY (cancelled == 0 AND diverted == 0 AND origin == 'ORD' AND dest != 'JFK');
flight2 = FILTER flight2 BY (cancelled == 0 AND diverted == 0 AND origin != 'ORD' AND dest == 'JFK');

-- join flights by (dest/origin, flightDate)
joined_flights = JOIN flight1 BY (dest, flightDate), flight2 BY (origin, flightDate);

-- filter out pairs that flight1.arrTime >= flight2.deptTime
joined_flights = FILTER joined_flights BY (flight1::arrTime < flight2::deptTime);

-- version 2: filter by only flight1's flightDate
joined_flights_in_range = FILTER joined_flights BY (
                (flight1::year == 2007 AND flight1::month >= 6) OR (flight1::year == 2008 AND flight1::month <= 5));
-- generate total_delay for each pair of two-leg flights
delay_min = FOREACH joined_flights_in_range GENERATE (flight1::arrDelayMin + flight2::arrDelayMin) AS total_delay;

-- group all to calculate average
delay_min_all = GROUP delay_min ALL;
avg_delay = FOREACH delay_min_all GENERATE AVG(delay_min.total_delay);

STORE avg_delay INTO '$OUTPUT';




