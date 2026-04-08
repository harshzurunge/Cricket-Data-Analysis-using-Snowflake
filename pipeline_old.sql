use role sysadmin;
use warehouse compute_wh;
use schema cricket.consumption;

create or replace stream cricket.raw.for_match_stream on table cricket.raw.match_raw_tbl append_only = true;
create or replace stream cricket.raw.for_player_stream on table cricket.raw.match_raw_tbl append_only = true;
create or replace stream cricket.raw.for_delivery_stream on table cricket.raw.match_raw_tbl append_only = true;


create or replace task cricket.raw.load_json_to_raw
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
    as
        copy into cricket.raw.match_raw_tbl from 
        (
            select 
                t.$1:meta::object as meta, 
                t.$1:info::variant as info, 
                t.$1:innings::array as innings, 
                --
                metadata$filename,
                metadata$file_row_number,
                metadata$file_content_key,
                metadata$file_last_modified
            from @cricket.land.my_stg/cricket/json (file_format => 'cricket.land.my_json_format') t
        )
        on_error = continue;


create or replace task cricket.raw.load_to_clean_match
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_json_to_raw
    when system$stream_has_data('cricket.raw.for_match_stream')
    as
    insert into cricket.clean.match_detail_clean 
        select
            info:match_type_number::int as match_type_number, 
            info:event.name::text as event_name,
            case
            when 
                info:event.match_number::text is not null then info:event.match_number::text
            when 
                info:event.stage::text is not null then info:event.stage::text
            else
                'NA'
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
        end as matach_result,
        case 
            when info:outcome.winner is not null then info:outcome.winner
            else 'NA'
        end as winner,   

            -- info:officials.match_referees[0]::text as match_referee,
            -- info:officials.reserve_umpires[0]::text as reserve_umpires,
            -- info:officials.tv_umpires[0]::text as tv_umpires,
            -- info:officials.umpires[0]::text as first_umpire,
            -- info:officials.umpires[1]::text as second_umpire,

            info:toss.winner::text as toss_winner,
            initcap(info:toss.decision::text) as toss_decision,
            --
            stg_file_name ,
            stg_file_row_number,
            stg_file_hashkey,
            stg_modified_ts
        from 
            cricket.raw.for_match_stream;


create or replace task cricket.raw.load_to_clean_player
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_clean_match
    when system$stream_has_data('cricket.raw.for_player_stream')
    as
    insert into cricket.clean.player_clean_tbl
    select 
        raw.info:match_type_number::int as match_type_number, 
        p.key::text as country,
        team.value:: text as player_name,
        raw.stg_file_name ,
        raw.stg_file_row_number,
        raw.stg_file_hashkey,
        raw.stg_modified_ts
    from cricket.raw.for_player_stream raw,
    lateral flatten (input => raw.info:players) p,
    lateral flatten (input => p.value) team; 


create or replace task cricket.raw.load_to_clean_delivery
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_clean_player
    when system$stream_has_data('cricket.raw.for_delivery_stream')
    as
    insert into cricket.clean.delivery_clean_tbl 
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
    from cricket.raw.for_delivery_stream m,
    lateral flatten (input => m.innings) i,
    lateral flatten (input => i.value:overs) o,
    lateral flatten(input => o.value:deliveries) d,
    lateral flatten (input => d.value:extras, outer => True) e,
    lateral flatten (input => d.value:wickets, outer => True) w;


create or replace task cricket.raw.load_to_team_dim
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
    as
    insert into cricket.consumption.team_dim(team_name) (
        select distinct team_name from(
        select first_team as team_name from cricket.clean.match_detail_clean
        union all
        select second_team as team_name from cricket.clean.match_detail_clean
        )
        minus
        select team_name from cricket.consumption.team_dim);


create or replace task cricket.raw.load_to_player_dim
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
    as
    insert into cricket.consumption.player_dim(team_id, player_name)
    (
        select b.team_id, a.player_name
        from 
            cricket.clean.player_clean_tbl a join cricket.consumption.team_dim b
            on a.country = b.team_name
        group by
            b.team_id,
            a.player_name
        minus
        select team_id, player_name from cricket.consumption.player_dim
    );


create or replace task cricket.raw.load_to_venue_dim
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
    as
    insert into cricket.consumption.venue_dim(venue_name, city)
    (
        select 
            venue, 
            case when city  is null  then 'NA'
            else city
            end as city 
        from 
            cricket.clean.match_detail_clean 
        group by 
            venue, city
        minus
        select venue_name, city from cricket.consumption.venue_dim
    );


create or replace task cricket.raw.load_to_match_fact
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_team_dim, cricket.raw.load_to_player_dim, cricket.raw.load_to_venue_dim
    as
    insert into cricket.consumption.match_fact 
    select a.* from 
    (
        select 
            m.match_type_number as match_id,
            dd.date_id as date_id,
            0 as referee_id,
            ftd.team_id as first_team_id,
            std.team_id as second_team_id,
            mtd.match_type_id as match_type_id,
            vd.venue_id as venue_id,
            50 as total_overs,
            6 as balls_per_overs,
            max(case when d.team_name = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
            sum(case when d.team_name = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
            sum(case when d.team_name = m.first_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_A,
            sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
            0 fours_by_team_a,
            0 sixes_by_team_a,
            (sum(case when d.team_name = m.first_team then  d.runs else 0 end ) + sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_A,
            sum(case when d.team_name = m.first_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_a,    
            
            max(case when d.team_name = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
            sum(case when d.team_name = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
            sum(case when d.team_name = m.second_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_B,
            sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
            0 fours_by_team_b,
            0 sixes_by_team_b,
            (sum(case when d.team_name = m.second_team then  d.runs else 0 end ) + sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_B,
            sum(case when d.team_name = m.second_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
            tw.team_id as toss_winner_team_id,
            m.toss_decision as toss_decision,
            m.matach_result as matach_result,
            mw.team_id as winner_team_id
            
        from 
            cricket.clean.match_detail_clean m
            join cricket.consumption.date_dim dd on m.event_date = dd.full_dt
            join cricket.consumption.team_dim ftd on m.first_team = ftd.team_name 
            join cricket.consumption.team_dim std on m.second_team = std.team_name 
            join cricket.consumption.match_type_dim mtd on m.match_type = mtd.match_type
            join cricket.consumption.venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
            join cricket.clean.delivery_clean_tbl d  on d.match_type_number = m.match_type_number 
            join cricket.consumption.team_dim tw on m.toss_winner = tw.team_name 
            join cricket.consumption.team_dim mw on m.winner= mw.team_name 
            --where m.match_type_number = 4686
        group by
            m.match_type_number,
            dd.date_id,
            referee_id,
            first_team_id,
            second_team_id,
            mtd.match_type_id,
            vd.venue_id,
            total_overs,
            toss_winner_team_id,
            m.toss_decision,
            m.match_result,
            winner_team_id
    ) a
    left join cricket.consumption.match_fact b on a.match_id = b.match_id
    where b.match_id is null    
    ;



create or replace task cricket.raw.load_delivery_fact
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_match_fact
    as
    insert into cricket.consumption.delivery_fact
    select a.* from
    (
        select 
            d.match_type_number as match_id,
            td.team_id,
            bpd.player_id as bower_id, 
            spd.player_id batter_id, 
            nspd.player_id as non_stricker_id,
            d.over,
            d.runs,
            case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
            case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
            case when d.player_out is null then 'None' else d.player_out end as player_out,
            case when d.player_out_kind is null then 'None' else d.player_out_kind end as player_out_kind
        from 
            cricket.clean.delivery_clean_tbl d
            join team_dim td on d.team_name = td.team_name
            join player_dim bpd on d.bowler = bpd.player_name
            join player_dim spd on d.batter = spd.player_name
            join player_dim nspd on d.non_striker = nspd.player_name
    ) a
    left join cricket.consumption.delivery_fact b on a.match_id = b.match_id 
    where b.match_id is null;


use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;



alter task cricket.raw.load_delivery_fact resume;
alter task cricket.raw.load_to_match_fact resume;
alter task cricket.raw.load_to_venue_dim resume;
alter task cricket.raw.load_to_player_dim resume;
alter task cricket.raw.load_to_team_dim resume;
alter task cricket.raw.load_to_clean_delivery resume;
alter task cricket.raw.load_to_clean_player resume;
alter task cricket.raw.load_to_clean_match resume;
alter task cricket.raw.load_json_to_raw resume;