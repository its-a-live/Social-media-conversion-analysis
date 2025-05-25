INSERT INTO DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)

select
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl."datetime" as event_dt,
	now() as load_dt,
	's3' as load_src
from STAGING.group_log as gl
left join DWH.h_groups as hg on gl.group_id = hg.group_id
left join DWH.h_users as hu on gl.user_id = hu.user_id
left join DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
order by luga.hk_l_user_group_activity asc;