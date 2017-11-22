import airflow
from datetime import timedelta, datetime, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor
from quboleWrapper import qubole_wrapper, export_to_rdms

query_type = 'dev_presto'

# Set expected runtime in seconds, setting to 0 is 7200 seconds
expected_runtime = 0

# The group that owns this DAG
owner = "Analytic Services"

default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 21),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_inventory_acquisition_dashboard',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(7, 00),
                             task_id='start_time_task',
                             dag=dag
                             )

current_date = (datetime.now()).date()
stats_date = current_date - timedelta(days=2)
stats_date2 = current_date - timedelta(days=1)

def qubole_operator(task_id, sql, retries, retry_delay, dag):
    return PythonOperator(
        task_id=task_id,
        python_callable=qubole_wrapper,
        provide_context=True,
        retries=retries,
        retry_delay=retry_delay,
        # schedule_interval=None,
        pool='presto_default_pool',
        op_kwargs={'db_type': query_type,
                   'raw_sql': sql,
                   'expected_runtime': expected_runtime,
                   'dag_id': dag.dag_id,
                   'task_id': task_id
                   },
        templates_dict={'ds': '{{ ds }}'},
        dag=dag)


def export_to_rdms_operator(task_id, table_name, retries, retry_delay, dag):
    return PythonOperator(
        task_id=task_id,
        python_callable=export_to_rdms,
        provide_context=True,
        retries=retries,
        retry_delay=retry_delay,
        pool='hive_default_pool',
        op_kwargs={'table_name': table_name,
                   'expected_runtime': expected_runtime,
                   'dag_id': dag.dag_id,
                   'task_id': task_id
                   },
        templates_dict=None,
        dag=dag)

insert_daily_inventory_acquisition_sql = '''Insert overwrite table as_shared.s2_inventory_acquisition_dashboard 

with loot_table as 
(
select name, reference, description, rarity, 'Launch' as productionlevel, category, rarity_s, loot_id, loot_group , BaseWeaponReference as weapon_base, (case when collectionrewardid = loot_id then 2
    when collectionid > 0 then 1 else 0 end) as is_collectible
from as_s2.loot_v4_ext a 
where productionlevel in ('Gold', 'TU1') 
AND trim(isloot) <> ''
and category in ('consumable', 'emote', 'grip', 'uniforms', 'weapon', 'playercard_title') 
and reference not in ('playercard_social_3', 'playercard_zm_challenge_11', 'playercard_zm_challenge_01', 'playercard_social_4')
group by 1,2,3,4,5,6,7,8,9,10,11
),

loot_cross as 
(
select category, rarity, productionlevel, is_collectible 
, case when category = 'emote' and rarity = 0 and is_collectible = 1 then 30 
       when category = 'emote' and rarity = 1 and is_collectible = 1  then 16 
	   when category = 'emote' and rarity = 2 and is_collectible = 1 then 4 
	   when category = 'emote' and rarity = 3 and is_collectible = 1 then 1 
	   
	   when category = 'emote' and rarity = 1 and is_collectible = 0 then 3 
	   when category = 'emote' and rarity = 2 and is_collectible = 0 then 5 
	   when category = 'emote' and rarity = 3 and is_collectible = 0 then 6 
	   when category = 'emote' and rarity = 4 and is_collectible = 0 then 1 
	   
	   when category = 'grip' and rarity = 0 and is_collectible = 1 then 30 
       when category = 'grip' and rarity = 1 and is_collectible = 1  then 18 
	   when category = 'grip' and rarity = 2 and is_collectible = 1 then 10 
	   when category = 'grip' and rarity = 3 and is_collectible = 1 then 4 
	   
	   when category = 'grip' and rarity = 0 and is_collectible = 0 then 24 
	   when category = 'grip' and rarity = 1 and is_collectible = 0 then 21
	   when category = 'grip' and rarity = 2 and is_collectible = 0 then 17 
	   when category = 'grip' and rarity = 3 and is_collectible = 0 then 9 
	   when category = 'grip' and rarity = 4 and is_collectible = 0 then 1 
	   
	   when category = 'uniforms' and rarity = 2 and is_collectible = 1 then 15 
	   when category = 'uniforms' and rarity = 3 and is_collectible = 1 then 20 
	   
	   when category = 'uniforms' and rarity = 3 and is_collectible = 0 then 44 
	   when category = 'uniforms' and rarity = 4 and is_collectible = 0 then 30  
	   
	   when category = 'weapon' and rarity = 3 and is_collectible = 0 then 57 
	   when category = 'weapon' and rarity = 4 and is_collectible = 0 then 54 
	   	   
	   when category = 'playercard_title' and rarity = 0 and is_collectible = 1 then 29 
	   when category = 'playercard_title' and rarity = 1 and is_collectible = 1 then 22 
	   when category = 'playercard_title' and rarity = 2 and is_collectible = 1 then 19 
	   when category = 'playercard_title' and rarity = 3 and is_collectible = 1 then 4 
	   
	   when category = 'playercard_title' and rarity = 0 and is_collectible = 0 then 26 
	   when category = 'playercard_title' and rarity = 1 and is_collectible = 0 then 18
	   when category = 'playercard_title' and rarity = 2 and is_collectible = 0 then 8 
	   when category = 'playercard_title' and rarity = 3 and is_collectible = 0 then 10 
       when category = 'playercard_title' and rarity = 4 and is_collectible = 0 then 1 
	   else pool_size end as pool_size 
from 
	   
(
select category, rarity, productionlevel, is_collectible 
--, condition 
, count(distinct loot_id) as pool_size
from loot_table 
group by 1,2,3,4
) 
),

temp_inventory_data as 
(
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_awardproduct_data_userdatachanges_inventoryitems 
where dt <= date('%s')
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_consumeawards_data_userdatachanges_inventoryitems 
where dt <= date('%s')
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, 0, 0 , dt
from ads_ww2.fact_mkt_consumeinventoryitems_data_eventinfo_inventoryitems 
where dt <= date('%s')
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_consumeinventoryitems_data_userdatachanges_inventoryitems 
where dt <= date('%s')
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_durableprocess_data_userdatachanges_inventoryitems 
where dt <= date('%s')
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_durablerevoke_data_userdatachanges_inventoryitems 
where dt <= date('%s')
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_pawnitems_data_userdatachanges_inventoryitems 
where dt <= date('%s')
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_purchaseskus_data_userdatachanges_inventoryitems 
where dt <= date('%s')
)

select 'Non-Spenders' as player_type, x.category, x.rarity, x.is_collectible, x.productionlevel, x.pool_size, z.unique_users, y.num_items, y.num_items*pow(z.unique_users, -1), z.dt as raw_date 

from loot_cross x 
join 
(select  category, productionlevel, rarity, is_collectible, sum(num_items) as num_items 
from 
(
select a.context_headers_user_id_s, b.category, b.productionlevel, b.rarity, b.is_collectible, count(distinct a.item_id_l) as num_items, sum(quantity_new_l - quantity_old_l ) as quantity_tot 

from 
temp_inventory_data a 
join loot_table b 
    on a.item_id_l = b.loot_id 
join (
     select distinct context_headers_title_id_s, context_headers_user_id_s 
	 from temp_inventory_data 
	 where dt = date('%s') ) c 
    on a.context_headers_title_id_s = c.context_headers_title_id_s 
    and a.context_headers_user_id_s = c.context_headers_user_id_s
group by 1,2,3,4,5
) 
group by 1,2,3,4) y 
on x.category = y.category 
and x.rarity = y.rarity 
and x.is_collectible = y.is_collectible

join 
    (select dt, sum(unique_users) as unique_users 
	from 
	(select dt, context_headers_title_id_s, count(distinct context_headers_user_id_s) as unique_users from temp_inventory_data where dt = date('%s') 
    group by 1,2) 
    group by 1 ) Z 
on 1=1 ''' %(stats_date, start_date, stats_date, stats_date, stats_date, stats_date, stats_date, stats_date, stats_date, stats_date) 

insert_daily_inventory_acquisition_task = qubole_operator('insert_daily_inventory_acquisition',
                                              insert_daily_inventory_acquisition_sql, 2, timedelta(seconds=600), dag)
# Wire up the DAG
insert_daily_inventory_acquisition_task.set_upstream(start_time_task)