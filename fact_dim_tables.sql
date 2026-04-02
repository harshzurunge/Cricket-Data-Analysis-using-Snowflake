use role sysadmin;
use warehouse compute_wh;
use schema cricket.consumption;

-- date-dim

create or replace table date_dim (
    date_id int primary key autoincrement,
    full_dt date,
    day int,
    month int,
    year int,
    quarter int,
    dayofweek int,
    dayofmonth int,
    dayofyear int,
    dayofweekname varchar(3), -- to store day names (e.g., "Mon")
    isweekend boolean -- to indicate if it's a weekend (True/False Sat/Sun both falls under weekend)
);

-------------------------------------------------------------------------------------------------------
-- referee dim

create or replace table referee_dim (
    referee_id int primary key autoincrement,
    referee_name text not null,
    referee_type text not null
);

insert into cricket.consumption.referee_dim 

    
------------------------------------------------------------------------------------------------------
-- team_dim

create or replace table team_dim (
    team_id int primary key autoincrement,
    team_name text not null
);

insert into cricket.consumption.team_dim(team_name)
select distinct team_name from(
select first_team as team_name from cricket.clean.match_detail_clean
union all
select second_team as team_name from cricket.clean.match_detail_clean
)order by team_name;

------------------------------------------------------------------------------------------------------
-- player dim
create or replace table player_dim (
    player_id int primary key autoincrement,
    team_id int not null,
    player_name text not null
);

alter table cricket.consumption.player_dim
add constraint fk_team_player_id
foreign key (team_id)
references cricket.consumption.team_dim (team_id);

-- select * from cricket.consumption.player_dim;

insert into cricket.consumption.player_dim(team_id, player_name)
select b.team_id, a.player_name
from 
    cricket.clean.player_clean_tbl a join cricket.consumption.team_dim b
    on a.country = b.team_name
group by
    b.team_id,
    a.player_name;

----------------------------------------------------------------------------------------------
-- venue dim

create or replace table venue_dim (
    venue_id int primary key autoincrement,
    venue_name text not null,
    city text not null,
    state text,
    country text,
    continent text,
    end_Names text,
    capacity number,
    pitch text,
    flood_light boolean,
    established_dt date,
    playing_area text,
    other_sports text,
    curator text,
    lattitude number(10,6),
    longitude number(10,6)
);

insert into cricket.consumption.venue_dim(venue_name, city)
select 
    venue, 
    case when city  is null  then 'NA'
    else city
    end as city 
from 
    cricket.clean.match_detail_clean 
group by 
    venue, city;


------------------------------------------------------------------------------------------
-- match dim

create or replace table match_type_dim (
    match_type_id int primary key autoincrement,
    match_type text not null
);

insert into  cricket.consumption.match_type_dim (match_type)
    select match_type from cricket.clean.match_detail_clean group by match_type;
 
------------------------------------------------------------------------------------------
-- match fact

CREATE or replace TABLE match_fact (
    match_id INT PRIMARY KEY autoincrement,
    date_id INT NOT NULL,
    referee_id INT NOT NULL,
    team_a_id INT NOT NULL,
    team_b_id INT NOT NULL,
    match_type_id INT NOT NULL,
    venue_id INT NOT NULL,
    total_overs number(3),
    balls_per_over number(1),

    overs_played_by_team_a number(2),
    bowls_played_by_team_a number(3),
    extra_bowls_played_by_team_a number(3),
    extra_runs_scored_by_team_a number(3),
    fours_by_team_a number(3),
    sixes_by_team_a number(3),
    total_score_by_team_a number(3),
    wicket_lost_by_team_a number(2),

    overs_played_by_team_b number(2),
    bowls_played_by_team_b number(3),
    extra_bowls_played_by_team_b number(3),
    extra_runs_scored_by_team_b number(3),
    fours_by_team_b number(3),
    sixes_by_team_b number(3),
    total_score_by_team_b number(3),
    wicket_lost_by_team_b number(2),

    toss_winner_team_id int not null, 
    toss_decision text not null, 
    match_result text not null, 
    winner_team_id int not null,

    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES date_dim (date_id),
    CONSTRAINT fk_referee FOREIGN KEY (referee_id) REFERENCES referee_dim (referee_id),
    CONSTRAINT fk_team1 FOREIGN KEY (team_a_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_team2 FOREIGN KEY (team_b_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_match_type FOREIGN KEY (match_type_id) REFERENCES match_type_dim (match_type_id),
    CONSTRAINT fk_venue FOREIGN KEY (venue_id) REFERENCES venue_dim (venue_id),

    CONSTRAINT fk_toss_winner_team FOREIGN KEY (toss_winner_team_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_winner_team FOREIGN KEY (winner_team_id) REFERENCES team_dim (team_id)
);

-----------------------------------------------------------------------------------------------------------
-- date_range temp table 

-- select min(event_date), max(event_date) from cricket.clean.match_detail_clean;


CREATE or replace transient TABLE cricket.clean.date_rnage01 (Date DATE);
insert into cricket.clean.date_rnage01 (date) values
('2023-10-12'), ('2023-10-13'), ('2023-10-14'), ('2023-10-15'), ('2023-10-16'), ('2023-10-17'), ('2023-10-18'), ('2023-10-19'), ('2023-10-20'), ('2023-10-21'), ('2023-10-22'), ('2023-10-23'), ('2023-10-24'), ('2023-10-25'), ('2023-10-26'), ('2023-10-27'), ('2023-10-28'), ('2023-10-29'), ('2023-10-30'), ('2023-10-31'), ('2023-11-01'), ('2023-11-02'), ('2023-11-03'), ('2023-11-04'), ('2023-11-05'), ('2023-11-06'), ('2023-11-07'), ('2023-11-08'), ('2023-11-09'), ('2023-11-10'), ('2023-11-11'), ('2023-11-12'), ('2023-11-13'), ('2023-11-14'), ('2023-11-15'), ('2023-11-16'), ('2023-11-17'), ('2023-11-18'), ('2023-11-19'), ('2023-11-20'), ('2023-11-21'), ('2023-11-22'), ('2023-11-23'), ('2023-11-24'), ('2023-11-25'), ('2023-11-26'), ('2023-11-27'), ('2023-11-28'), ('2023-11-29'), ('2023-11-30'), ('2023-12-01'), ('2023-12-02'), ('2023-12-03'), ('2023-12-04'), ('2023-12-05'), ('2023-12-06'), ('2023-12-07'), ('2023-12-08'), ('2023-12-09'), ('2023-12-10'), ('2023-12-11'), ('2023-12-12'), ('2023-12-13'), ('2023-12-14'), ('2023-12-15'), ('2023-12-16'), ('2023-12-17'), ('2023-12-18'), ('2023-12-19'), ('2023-12-20'), ('2023-12-21'), ('2023-12-22'), ('2023-12-23'), ('2023-12-24'), ('2023-12-25'), ('2023-12-26'), ('2023-12-27'), ('2023-12-28'), ('2023-12-29'), ('2023-12-30'), ('2023-12-31');


INSERT INTO cricket.consumption.date_dim (Date_ID, Full_Dt, Day, Month, Year, Quarter, DayOfWeek, DayOfMonth, DayOfYear, DayOfWeekName, IsWeekend)
SELECT
    ROW_NUMBER() OVER (ORDER BY Date) AS DateID,
    Date AS FullDate,
    EXTRACT(DAY FROM Date) AS Day,
    EXTRACT(MONTH FROM Date) AS Month,
    EXTRACT(YEAR FROM Date) AS Year,
    CASE WHEN EXTRACT(QUARTER FROM Date) IN (1, 2, 3, 4) THEN EXTRACT(QUARTER FROM Date) END AS Quarter,
    DAYOFWEEKISO(Date) AS DayOfWeek,
    EXTRACT(DAY FROM Date) AS DayOfMonth,
    DAYOFYEAR(Date) AS DayOfYear,
    DAYNAME(Date) AS DayOfWeekName,
    CASE When DAYNAME(Date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS IsWeekend
FROM cricket.clean.date_rnage01;


