create or replace view cricket.gold.player as
select
    match_type_number,
    country,
    player_name
from cricket.silver.player_clean p;

create or replace view cricket.gold.delivery as
select
    match_type_number,
    team_name,
    over,
    bowler,
    batter,
    non_striker,
    runs,
    extras,
    extra_type,
    total,
    player_out,
    player_out_kind,
    player_out_fielder,
    review_by_team,
    review_decision,
    review_type
from cricket.silver.delivery_clean d;


create or replace view cricket.gold.match as
select
    match_type_number,
    event_name,
    match_number,
    event_date,
    event_year,
    event_month,
    event_day,
    match_type,
    season,
    team_type,
    overs,
    city,
    venue,
    gender,
    first_team,
    second_team,
    winner,
    won_by_runs,
    won_by_wickets,
    player_of_match,
    match_referee,
    reserve_umpires,
    tv_umpires,
    first_umpire,
    second_umpire,
    toss_winner,
    toss_decision
from cricket.silver.match_clean m;