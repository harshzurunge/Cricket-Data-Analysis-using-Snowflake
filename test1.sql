select match_type_number, over, runs, extras, total from cricket.clean.delivery_clean_tbl
where match_type_number = 4673 and over = 1;

select match_id, over, runs, extra_runs from cricket.consumption.delivery_fact
where match_id = 4673 and over = 1;



select * from cricket.clean.delivery_clean_tbl limit 50;


select count(distinct player_name) from cricket.clean.player_clean_tbl;

select * from cricket.clean.delivery_clean_tbl where match_type_number = 4667
and over = 17 and team_name = 'Australia' limit 100;


select
  o.value:over::int as over,
  count(*) as raw_delivery_rows,
  count(distinct d.seq) as distinct_deliveries
from cricket.raw.match_raw_tbl m
,lateral flatten(input=>m.innings) i
,lateral flatten(input=>i.value:overs) o
,lateral flatten(input=>o.value:deliveries) d
group by over
order by over;


select * from cricket.clean.delivery_clean_tbl
where match_type_number = 4667;

select distinct(extra_type) from cricket.consumption.delivery_fact
where match_id = 4667;

select * from cricket.clean.delivery_clean_tbl
where match_type_number = 4667 and over = 31;


select * from cricket.clean.match_detail_clean;

select * from cricket.silver.player_clean limit 100;