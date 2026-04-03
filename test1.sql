select match_type_number, over, runs, extras, total from cricket.clean.delivery_clean_tbl
where match_type_number = 4673 and over = 1;

select match_id, over, runs, extra_runs from cricket.consumption.delivery_fact
where match_id = 4673 and over = 1;