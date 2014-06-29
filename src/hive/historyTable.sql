SET hive.base.inputformat=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.exec.reducers.bytes.per.reducer = 1000000000;
SET mapred.map.tasks=48;
SET mapred.reduce.tasks = -1;
SET hive.exec.reducers.max = 999;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE historyTable
(USAF BIGINT, WBAN BIGINT, STATION_NAME STRING, CTRY STRING, FIPS STRING, STATE STRING, CALL STRING, 
LATITUDE DOUBLE, LONGITUDE STRING, ELEV STRING, BEGIN BIGINT, ENDDATE BIGINT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/home/manali/historyTable/';


LOAD DATA LOCAL INPATH '/home/manali/workspace/mapreduce/ish-history.csv'
OVERWRITE INTO TABLE historyTable; 

create table maxTempTable(stn STRING,wban STRING, year STRING, month STRING, temp STRING, temp_count STRING, dewp STRING, dewp_count STRING, slp STRING, slp_count STRING, stp STRING, stp_count STRING, visib STRING, visib_count STRING, wdsp STRING, wdsp_count STRING, mxspd STRING,GUST STRING, maxtemp STRING, mintemp STRING, prcp STRING, sndp STRING, frshtt STRING ) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' 
with SERDEPROPERTIES (
"input.regex" = "(\\d+) (\\d+)\\s+(\\d{4})(\\d{2})\\d+\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\-?\\d+\\.?\\d+)\\*?\\s+(\\-?\\d+\\.?\\d+)\\*?\\s+(\\-?\\d+\\.?\\d+)\\w\\s+(\\-?\\d+\\.?\\d+)\\s+(\\-?\\d+\\.?\\d+)",
"output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s %13$s %14$s %15$s %16$s %17$s %18$s %19$s %20$s %21$s %22$s %23$s")
LOCATION '/home/manali/Documents/project/hive/data/2002Data/';

INSERT OVERWRITE DIRECTORY '/home/manali/Documents/project/hive/data/2002MaxTempLatFinal1/'
select h.* from
historyTable h join 
(select stn, year, max(cast(maxtemp as FLOAT)) from maxTempTable
where maxtemp!=9999.9 group by stn, year)maxFinal
on h.USAF = maxFinal.stn;





