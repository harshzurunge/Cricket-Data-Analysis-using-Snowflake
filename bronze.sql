use role sysadmin;
use warehouse compute_wh;
use database cricket;
use schema cricket.bronze;

-- Player_table
create or replace table cricket.bronze.player_ as 
select
    raw.info:match_type_number::int as match_type_number,
    p.key::text as country,
    team.value::text as player_name,

    raw.stg_file_name ,
    raw.stg_file_row_number,
    raw.stg_file_hashkey,
    raw.stg_modified_ts
from cricket.raw.match_raw_tbl raw,
lateral flatten (input => raw.info:players) p,
lateral flatten (input => p.value) team;

-------------------------------------------------------------------------
-- delivery_table

create or replace table cricket.bronze.delivery_table as
select
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.value:over::int as over,
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
    f.value:name::text as player_out_fielder,

    m.stg_file_name ,
    m.stg_file_row_number,
    m.stg_file_hashkey,
    m.stg_modified_ts
from cricket.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten(input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer => True) e,
lateral flatten (input => d.value:wickets, outer => True) w,
-- lateral flatten (input => d.value:review, outer => True) r,
lateral flatten (input => w.value:fielders, outer => True) f
;