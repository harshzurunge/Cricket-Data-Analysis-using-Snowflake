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
