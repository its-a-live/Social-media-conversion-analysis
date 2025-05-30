INSERT INTO DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)

select distinct
    hash(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hk_group_id,
	now() as load_dt,
	's3' as load_src
from STAGING.group_log as gl
left join DWH.h_users hu on gl.user_id = hu.user_id
left join DWH.h_groups hg on gl.group_id = hg.group_id ;