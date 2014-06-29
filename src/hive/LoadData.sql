add jar s3://dhotrem/hive/hive-contrib-0.8.1.jar;

SET hive.base.inputformat=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.exec.reducers.bytes.per.reducer = 1000000000;
SET mapred.map.tasks=23;
SET mapred.reduce.tasks = -1;
SET hive.exec.reducers.max = 999;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

create table Weather(stn STRING,wban STRING, year STRING, month STRING, temp STRING, temp_count STRING, dewp STRING, dewp_count STRING, slp STRING, slp_count STRING, stp STRING, stp_count STRING, visib STRING, visib_count STRING, wdsp STRING, wdsp_count STRING, mxspd STRING,GUST STRING, maxtemp STRING, mintemp STRING, prcp STRING, sndp STRING, frshtt STRING ) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' 
with SERDEPROPERTIES (
"input.regex" = "(\\d+) (\\d+)\\s+(\\d{4})(\\d{2})\\d+\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\-?\\d+\\.?\\d+)\\s+(\\-?\\d+\\.?\\d+)\\*?\\s+(\\-?\\d+\\.?\\d+)\\*?\\s+(\\-?\\d+\\.?\\d+)\\w\\s+(\\-?\\d+\\.?\\d+)\\s+(\\-?\\d+\\.?\\d+)",
"output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s %13$s %14$s %15$s %16$s %17$s %18$s %19$s %20$s %21$s %22$s %23$s")
LOCATION 's3://cs6240data/';
