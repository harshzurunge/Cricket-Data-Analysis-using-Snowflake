use role sysadmin;
use warehouse compute_wh;
use database cricket;
use schema cricket.bronze;

-- Player_table
create or replace table cricket.bronze.player_table as 
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

select * from cricket.bronze.player_table;

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

    d.value:review:by::text as review_by_team,
    d.value:review:decision::text as review_decision,
    d.value:review:type::text as review_type,

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
lateral flatten (input => w.value:fielders, outer => True) f;

select * from cricket.bronze.delivery_table limit 500;

-------------------------------------------------------------------------
-- match_table
create or replace transient table cricket.bronze.match_table as
select
    info:match_type_number::int as match_type_number, 
    info:event.name::text as event_name,

    -- case
    --     when info:event.match_number::text is not null then info:event.match_number::text
    --     when info:event.stage::text is not null then info:event.stage::text
    --     else 'NA'
    -- end as match_stage,  
    info:event.match_number :: int as match_number, 

    info:dates[0]::date as event_date,
    -- date_part('year',info:dates[0]::date) as event_year,
    -- date_part('month',info:dates[0]::date) as event_month,
    -- date_part('day',info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::int as overs,
    info:city::text as city,
    info:venue::text as venue, 
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,

    -- case 
    --     when info:outcome.winner is not null then 'Result Declared'
    --     when info:outcome.result = 'tie' then 'Tie'
    --     when info:outcome.result = 'no result' then 'No Result'
    --     else info:outcome.result
    -- end as match_result,
    -- case 
    --     when info:outcome.winner is not null then info:outcome.winner
    --     else 'NA'
    -- end as winner,
    info:outcome.winner::text as winner,
    info:outcome.by.runs::int as won_by_runs,
    info:outcome.by.wickets::int as won_by_wickets,
    info:player_of_match[0]::text as player_of_match,

    info:officials.match_referees[0]::text as match_referee,
    info:officials.reserve_umpires[0]::text as reserve_umpires,
    info:officials.tv_umpires[0]::text as tv_umpires,
    info:officials.umpires[0]::text as first_umpire,
    info:officials.umpires[1]::text as second_umpire,
    info:toss.winner::text as toss_winner,
    info:toss.decision::text as toss_decision,

    stg_file_name ,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
from 
    cricket.raw.match_raw_tbl;

select * from cricket.bronze.match_table;