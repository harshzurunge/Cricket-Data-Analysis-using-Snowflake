use role sysadmin;
use warehouse compute_wh;
use schema cricket.clean;

-----------------------------------------------------------------------------------------------------------
-- cricket.raw.match_raw_tbl raw

select 
    meta['data_version']::text as data_version,
    meta['created']::date as created,
    meta['revision']::number as revision
from
    cricket.raw.match_raw_tbl;

select 
    info:match_type_number :: int as match_type_number,
    info:match_type :: text as match_type,
    info:season :: text as season,
    info:team_type :: text as team_type,
    info:overs :: text as overs,
    info:city :: text as city,
    info:venue :: text as venue
from
    cricket.raw.match_raw_tbl;

select 
    raw.info:match_type_number :: int as match_type_number,
    raw.info:players,
    raw.info:teams
from cricket.raw.match_raw_tbl raw;


select 
    raw.info:match_type_number :: int as match_type_number,
    raw.info:players,
    raw.info:teams
from cricket.raw.match_raw_tbl raw
where match_type_number = 4673;

-----------------------------------------------------------------------------------
-- cricket.clean.delivery_clean_tbl
select
    m.info:match_type_number::int as match_type_number,
    m.innings
from cricket.raw.match_raw_tbl m
where match_type_number = 4673;


select
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    i.*
from cricket.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i
where match_type_number = 4673;

select
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.*
from cricket.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o
where match_type_number = 4673;

select
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.value:over::int+1 as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::number as extra_runs
from cricket.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten(input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer => True) e
where match_type_number = 4673;


select
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.value:over::int+1 as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::number as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders,
    m.stg_file_name ,
    m.stg_file_row_number,
    m.stg_file_hashkey,
    m.stg_modified_ts
from cricket.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten(input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer => True) e,
lateral flatten (input => d.value:wickets, outer => True) w;


select distinct match_type_number from cricket.clean.delivery_clean_tbl;