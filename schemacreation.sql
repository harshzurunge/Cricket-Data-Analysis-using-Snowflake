use role sysadmin;
use warehouse compute_wh;

create database if not exists cricket;
use database cricket;

create or replace schema land;
create or replace schema raw;
create or replace schema clean;
create or replace schema consumption;

use schema cricket.land;

-- json file format
create or replace file format cricket.land.my_json_format
type = json
null_if = ('\\n', 'null', '')
strip_outer_array = true
comment = 'Json File Format with outer stip array flag true'; 

-- creating an internal stage
create or replace stage cricket.land.my_stg; 

-- quick check if data is coming correctly or not
select 
        t.$1:meta::variant as meta, 
        t.$1:info::variant as info, 
        t.$1:innings::array as innings, 
        metadata$filename as file_name,
        metadata$file_row_number int,
        metadata$file_content_key text,
        metadata$file_last_modified stg_modified_ts
     from  @my_stg/cricket/json/1384401.json (file_format => 'my_json_format') t;


