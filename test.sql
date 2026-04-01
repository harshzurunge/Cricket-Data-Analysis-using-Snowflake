use role sysadmin;
use warehouse compute_wh;
use schema cricket.clean;

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

