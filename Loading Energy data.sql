-- Create database
Create or replace database ENERGYCONSUMPTIONDB;

--create temporary tables
create or replace table uk_load(
    utc_timestamp DATETIME,
    consumption FLOAT 
);


-- create file format object
create or replace file format mycsvformat
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1;



-- create stage objects
create or replace stage my_csv_stage
    file_format = mycsvformat;



--Staging the csv file
PUT file:///Users/brunchcode/tmp/load/UK_load.csv @my_csv_stage AUTO_COMPRESS=TRUE;



list @my_csv_stage;



--Copy staged files into tables in the database
copy into uk_load
    from @my_csv_stage/UK_load.csv.gz
    file_format =(format_name = mycsvformat)
    on_error = 'skip_file';


select * 
from uk_load;




--Remove staged files
remove @my_csv_stage pattern='.*.csv.gz';

remove @my_json_stage pattern='.*.json.gz';


drop database if exists mydatabase;