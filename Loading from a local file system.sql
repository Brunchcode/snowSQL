-- Create database
Create or replace database mydatabase;

--create temporary tables
create or replace temporary table mycsvtable(
    id INTEGER,
    last_name STRING,
    first_name STRING,
    company STRING,
    email STRING,
    workphone STRING,
    cellphone STRING,
    streetaddress STRING,
    city STRING,
    postalcode STRING
);


create or replace temporary table myjsontable(
json_data variant
);



-- create file format object
create or replace file format mycsvformat
    type = 'CSV'
    field_delimiter = '|'
    skip_header = 1;

create or replace file format myjsonformat
    type = 'JSON'
    strip_outer_array = true;


-- create stage objects
create or replace stage my_csv_stage
    file_format = mycsvformat;

create or replace stage my_json_stage
    file_format = myjsonformat;

--Staging the csv file
PUT file:///Users/brunchcode/tmp/load/contacts*.csv @my_csv_stage AUTO_COMPRESS=TRUE;

--Staging the json file
PUT file:///Users/brunchcode/tmp/load/contacts.json @my_json_stage AUTO_COMPRESS = TRUE;

list @my_csv_stage;

list @my_json_stage;


--Copy staged files into tables in the database
copy into MYCSVTABLE
    from @my_csv_stage/contacts1.csv.gz
    file_format =(format_name = mycsvformat)
    on_error = 'skip_file';

--multiple files
copy into MYCSVTABLE
    from @my_csv_stage
    file_format =(format_name = mycsvformat)
    pattern ='.*contacts[1-5].csv.gz'
    on_error = 'skip_file';

--json
copy into MYJSONTABLE
    from @my_json_stage/contacts.json.gz
    file_format =(format_name = myjsonformat)
    on_error = 'skip_file';


--resolve data load errors
CREATE OR REPLACE TABLE save_copy_errors AS SELECT * FROM TABLE(VALIDATE(mycsvtable, JOB_ID=>'01b40f46-0000-e118-0000-00019e528b69'));

SELECT * FROM SAVE_COPY_ERRORS;

--fix the errors manually

--stage

PUT file:///Users/brunchcode/tmp/load/contacts3.csv @my_csv_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

--copy corrected staged file into database tables
copy into MYCSVTABLE
    from @my_csv_stage/contacts3.csv.gz
    file_format =(format_name = mycsvformat)
    on_error = 'skip_file';


--check or verify
select * 
from mycsvtable;

select * 
from myjsontable;


--Remove staged files
remove @my_csv_stage pattern='.*.csv.gz';

remove @my_json_stage pattern='.*.json.gz';


drop database if exists mydatabase;