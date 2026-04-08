use role sysadmin;
use warehouse compute_wh;
use schema cricket.clean;

-- create or replace table cricket.clean.data_quality_rejects as
-- select
--     current_timestamp() as rejection_ts,
--     source_table,
--     target_table,
--     rejection_reason,
--     stg_file_name,
--     stg_file_row_number,
--     stg_file_hashkey,
--     stg_modified_ts,
--     raw_info
-- from values (null);

create or replace transient table cricket.clean.match_detail_clean as
select
    info:match_type_number::int as match_type_number, 
    info:event.name::text as event_name,
    case
        when info:event.match_number::text is not null then info:event.match_number::text
        when info:event.stage::text is not null then info:event.stage::text
        else 'NA'
    end as match_stage,   
    info:dates[0]::date as event_date,
    date_part('year',info:dates[0]::date) as event_year,
    date_part('month',info:dates[0]::date) as event_month,
    date_part('day',info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue, 
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case 
        when info:outcome.winner is not null then 'Result Declared'
        when info:outcome.result = 'tie' then 'Tie'
        when info:outcome.result = 'no result' then 'No Result'
        else info:outcome.result
    end as match_result,
    case 
        when info:outcome.winner is not null then info:outcome.winner
        else 'NA'
    end as winner,
    info:officials.match_referees[0]::text as match_referee,
    info:officials.reserve_umpires[0]::text as reserve_umpires,
    info:officials.tv_umpires[0]::text as tv_umpires,
    info:officials.umpires[0]::text as first_umpire,
    info:officials.umpires[1]::text as second_umpire,
    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision,
    stg_file_name ,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
from 
    cricket.raw.match_raw_tbl
where info:match_type_number::int is not null;

-- insert into cricket.clean.data_quality_rejects
-- select
--     current_timestamp() as rejection_ts,
--     'cricket.raw.match_raw_tbl' as source_table,
--     'match_detail_clean' as target_table,
--     'match_type_number_null' as rejection_reason,
--     stg_file_name,
--     stg_file_row_number,
--     stg_file_hashkey,
--     stg_modified_ts,
--     info as raw_info
-- from cricket.raw.match_raw_tbl
-- where info:match_type_number::int is null;

alter table cricket.clean.match_detail_clean
add constraint pk_match_type_number primary key (match_type_number);

----------------------------------------------------------------------------------------------------
    
create or replace table cricket.clean.player_clean_tbl as 
select 
    raw.info:match_type_number::int as match_type_number,
    case 
        when trim(coalesce(p.key::text, '')) = '' then 'Unknown'
        else initcap(trim(p.key::text))
    end as country,

    case 
        when trim(coalesce(team.value::text, '')) = '' then 'Unknown'
        else initcap(trim(team.value::text))
    end as player_name,

    raw.stg_file_name ,
    raw.stg_file_row_number,
    raw.stg_file_hashkey,
    raw.stg_modified_ts
from cricket.raw.match_raw_tbl raw,
lateral flatten (input => raw.info:players) p,
lateral flatten (input => p.value) team
where raw.info:match_type_number::int is not null;

-- insert into cricket.clean.data_quality_rejects
-- select
--     current_timestamp() as rejection_ts,
--     'cricket.raw.match_raw_tbl' as source_table,
--     'player_clean_tbl' as target_table,
--     'match_type_number_null' as rejection_reason,
--     stg_file_name,
--     stg_file_row_number,
--     stg_file_hashkey,
--     stg_modified_ts,
--     info as raw_info
-- from cricket.raw.match_raw_tbl
-- where info:match_type_number::int is null;

alter table cricket.clean.player_clean_tbl
modify column match_type_number set not null;

alter table cricket.clean.player_clean_tbl
modify column country set not null;

alter table cricket.clean.player_clean_tbl
modify column player_name set not null;

alter table cricket.clean.player_clean_tbl
add constraint fk_match_id
foreign key (match_type_number)
references cricket.clean.match_detail_clean (match_type_number);


select get_ddl('table', 'cricket.clean.match_detail_clean');

----------------------------------------------------------------------------------------------------

create or replace table cricket.clean.delivery_clean_tbl as
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

    -- r.value:by::text as review_by_team,
    -- r.value:decision::text as review_decision,
    -- r.value:type::text as review_type,

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



alter table cricket.clean.delivery_clean_tbl
modify column match_type_number set not null;

alter table cricket.clean.delivery_clean_tbl
modify column team_name set not null;

alter table cricket.clean.delivery_clean_tbl
modify column over set not null;

alter table cricket.clean.delivery_clean_tbl
modify column bowler set not null;

alter table cricket.clean.delivery_clean_tbl
modify column batter set not null;

alter table cricket.clean.delivery_clean_tbl
modify column non_striker set not null;

-- fk relationship
alter table cricket.clean.delivery_clean_tbl
add constraint fk_delivery_match_id
foreign key (match_type_number)
references cricket.clean.match_detail_clean (match_type_number);


----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
-- old
-- bronze

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

-------------------------------------------------------------------------
-- match_table
create or replace transient table cricket.bronze.match_table as
select
    info:match_type_number::int as match_type_number, 
    info:event.name::text as event_name,

    info:event.match_number :: int as match_number, 
    info:dates[0]::date as event_date,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::int as overs,
    info:city::text as city,
    info:venue::text as venue, 
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,

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

----------------------------------------------------------------------------------------------------
-- silver 
use database cricket;
use role sysadmin;
use warehouse compute_wh;  

create or replace schema cricket.silver;
use schema cricket.silver;

--player_clean
create or replace table cricket.silver.player_clean as
select
    p.match_type_number::int as match_type_number,
    case
        when trim(coalesce(regexp_replace(p.country, '\\s+', ' '), '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(p.country, '\\s+', ' ')))
    end as country,
    case
        when trim(coalesce(regexp_replace(p.player_name, '\\s+', ' '), '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(p.player_name, '\\s+', ' ')))
    end as player_name,

    p.stg_file_name,
    p.stg_file_hashkey,
    p.stg_modified_ts
from cricket.bronze.player_table p
where p.match_type_number is not null;


-----------------------------------------------------------------------
-- delivery_clean

create or replace table cricket.silver.delivery_clean as
select
    d.match_type_number::int as match_type_number,
    case
        when trim(coalesce(d.team_name::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(d.team_name::text, '\\s+', ' ')))
    end as team_name,
    coalesce(try_to_number(d.over::text), 0000) as over,
    case
        when trim(coalesce(d.bowler::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(d.bowler::text, '\\s+', ' ')))
    end as bowler,
    case
        when trim(coalesce(d.batter::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(d.batter::text, '\\s+', ' ')))
    end as batter,
    case
        when trim(coalesce(d.non_striker::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(d.non_striker::text, '\\s+', ' ')))
    end as non_striker,
    coalesce(try_to_number(d.runs::text), 0000) as runs,
    coalesce(try_to_number(d.extras::text), 0000) as extras,
    case
        when trim(coalesce(d.extra_type::text, '')) = '' then 'None'
        else initcap(trim(regexp_replace(d.extra_type::text, '\\s+', ' ')))
    end as extra_type,
    coalesce(try_to_number(d.total::text), 0000) as total,
    case
        when trim(coalesce(d.player_out::text, '')) = '' then 'None'
        else initcap(trim(regexp_replace(d.player_out::text, '\\s+', ' ')))
    end as player_out,
    case
        when trim(coalesce(d.player_out_kind::text, '')) = '' then 'None'
        else initcap(trim(regexp_replace(d.player_out_kind::text, '\\s+', ' ')))
    end as player_out_kind,
    case
        when trim(coalesce(d.player_out_fielder::text, '')) = '' then 'None'
        else initcap(trim(regexp_replace(d.player_out_fielder::text, '\\s+', ' ')))
    end as player_out_fielder,

    case
        when trim(coalesce(d.review_by_team::text, '')) = '' then 'None'
        else initcap(trim(regexp_replace(d.review_by_team::text, '\\s+', ' ')))
    end as review_by_team,
    case
        when trim(coalesce(d.review_decision::text, '')) = '' then 'None'
        else initcap(trim(regexp_replace(d.review_decision::text, '\\s+', ' ')))
    end as review_decision,
    case
        when trim(coalesce(d.review_type::text, '')) = '' then 'None'
        else initcap(trim(regexp_replace(d.review_type::text, '\\s+', ' ')))
    end as review_type,

    d.stg_file_name ,
    d.stg_file_row_number,
    d.stg_file_hashkey,
    d.stg_modified_ts
from cricket.bronze.delivery_table d
where d.match_type_number is not null;

--------------------------------------------------------------------------------
-- match_clean

create or replace table cricket.silver.match_clean as
select
    m.match_type_number::int as match_type_number,
    case
        when trim(coalesce(m.event_name::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.event_name::text, '\\s+', ' ')))
    end as event_name,
    coalesce(m.match_number::int, 0000) as match_number,
    coalesce(m.event_date::date, to_date('1900-01-01', 'YYYY-MM-DD')) as event_date,
    date_part('year', coalesce(m.event_date::date, to_date('1900-01-01', 'YYYY-MM-DD'))) as event_year,
    date_part('month', coalesce(m.event_date::date, to_date('1900-01-01', 'YYYY-MM-DD'))) as event_month,
    date_part('day', coalesce(m.event_date::date, to_date('1900-01-01', 'YYYY-MM-DD'))) as event_day,
    case
        when trim(coalesce(m.match_type::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.match_type::text, '\\s+', ' ')))
    end as match_type,
    case
        when trim(coalesce(m.season::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.season::text, '\\s+', ' ')))
    end as season,
    case
        when trim(coalesce(m.team_type::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.team_type::text, '\\s+', ' ')))
    end as team_type,
    coalesce(m.overs::int, 0000) as overs,
    case
        when trim(coalesce(m.city::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.city::text, '\\s+', ' ')))
    end as city,
    case
        when trim(coalesce(m.venue::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.venue::text, '\\s+', ' ')))
    end as venue,
    case
        when trim(coalesce(m.gender::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.gender::text, '\\s+', ' ')))
    end as gender,
    case
        when trim(coalesce(m.first_team::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.first_team::text, '\\s+', ' ')))
    end as first_team,
    case
        when trim(coalesce(m.second_team::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.second_team::text, '\\s+', ' ')))
    end as second_team,
    case
        when trim(coalesce(m.winner::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.winner::text, '\\s+', ' ')))
    end as winner,
    coalesce(m.won_by_runs::int, 0000) as won_by_runs,
    coalesce(m.won_by_wickets::int, 0000) as won_by_wickets,
    case
        when trim(coalesce(m.player_of_match::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.player_of_match::text, '\\s+', ' ')))
    end as player_of_match,
    case
        when trim(coalesce(m.match_referee::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.match_referee::text, '\\s+', ' ')))
    end as match_referee,
    case
        when trim(coalesce(m.reserve_umpires::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.reserve_umpires::text, '\\s+', ' ')))
    end as reserve_umpires,
    case
        when trim(coalesce(m.tv_umpires::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.tv_umpires::text, '\\s+', ' ')))
    end as tv_umpires,
    case
        when trim(coalesce(m.first_umpire::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.first_umpire::text, '\\s+', ' ')))
    end as first_umpire,
    case
        when trim(coalesce(m.second_umpire::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.second_umpire::text, '\\s+', ' ')))
    end as second_umpire,
    case
        when trim(coalesce(m.toss_winner::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.toss_winner::text, '\\s+', ' ')))
    end as toss_winner,
    case
        when trim(coalesce(m.toss_decision::text, '')) = '' then 'Unknown'
        else initcap(trim(regexp_replace(m.toss_decision::text, '\\s+', ' ')))
    end as toss_decision,

    m.stg_file_name,
    m.stg_file_row_number,
    m.stg_file_hashkey,
    m.stg_modified_ts
from 
    cricket.bronze.match_table m
where m.match_type_number::int is not null;
