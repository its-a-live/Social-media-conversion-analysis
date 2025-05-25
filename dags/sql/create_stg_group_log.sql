drop table if exists STAGING.group_log;

create table STAGING.group_log(
group_id int primary key,
user_id int,
user_id_from int,
event varchar(20),
"datetime" timestamp
)
ORDER BY group_id, user_id
SEGMENTED BY hash(group_id) all nodes
PARTITION BY "datetime"::date
GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2);
