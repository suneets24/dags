import airflow
from datetime import timedelta, datetime, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor
from quboleWrapper import qubole_wrapper, export_to_rdms, spark_wrapper

query_type = 'dev_presto'

# Set expected runtime in seconds, setting to 0 is 7200 seconds
expected_runtime = 0

# The group that owns this DAG
owner = "Analytic Services"

default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': datetime(2017, 12, 01),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_division_usage',
                  default_args=default_args
                  )

				  
				  
# Start running at this time
start_time_task = TimeSensor(target_time=time(7, 15),
                             task_id='start_time_task',
                             dag=dag
                             )

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

def spark_operator(label, task_id, program, language, arguments, retries, retry_delay, dag):
    return PythonOperator(
	    task_id=task_id,
		python_callable=spark_wrapper,
		provide_context=True,
		retries=retries,
		retry_delay=retry_delay,
		arguments=arguments,
		pool='spark_default_pool',
		op_kwargs={'label': label,
		           'program': program,
				   'language': language,
				   'arguments': arguments,
				   'expected_runtime':expected_runtime,
				   'dag_id': dag.dag_id,
				   'task_id': task_id
		           },
		templates_dict={'ds': '{{ ds }}'},
		dag=dag)

insert_division_usage_sql = """

Insert overwrite table as_s2.s2_divisions_dashboard
with loot_table as 
(
select name, reference, description, rarity, 'Launch' as productionlevel, category, rarity_s, loot_id, loot_group , BaseWeaponReference as weapon_base, (case when collectionid is not null then 1 else 0 end) as is_collectible
from as_s2.loot_v4_ext a 
where productionlevel in ('Gold', 'TU1', 'MTX1') 
AND trim(isloot) <> '' 
and category in ('weapon') 
group by 1,2,3,4,5,6,7,8,9,10,11
),

temp_div_lives as 
(
-- Aggregate Division use duration, num_spawns from Lives data

select y.dt 
, y.context_headers_title_id_s as platform
, y.context_data_match_common_matchid_s 
, y.victim_user_id
, y.weapon_base 
, y.weapon_class
, y.division_l 
,y.player_rank
,y.player_prestige
, count(*) as num_spawns
, sum(y.death_time_ms_i - y.spawn_time_ms_i)/1000.0 as duration_played 

from 
(
select distinct b.context_headers_title_id_s
, a.victim_weapon_s 
, d.weapon_base 
, d.loot_group as weapon_class 
, a.victim_weapon_guid_l
, a.attacker_weapon_s
, a.context_headers_event_id_s 
, a.context_data_match_common_matchid_s
, a.victim_user_id
, a.victim_loadout_index_i
, a.attacker_user_id
, a.spawn_time_ms_i 
, a.death_time_ms_i
, a.duration_ms_i 
, c.division_l
,b.context_data_players_start_rank_i as player_rank
,b.context_data_players_prestige_i as player_prestige
, b.dt 
from 
( select * from ads_ww2.fact_mp_match_data_lives where dt =  date '{{DS_DATE_ADD(0)}}' and context_data_match_common_is_private_match_b = FALSE )a 
join 
(
    select distinct context_headers_title_id_s, context_data_match_common_matchid_s,context_data_players_client_user_id_l,context_data_players_prestige_i,context_data_players_start_rank_i,  
	context_headers_event_id_s, context_data_match_common_utc_start_time_i, context_data_match_common_life_count_i, 
	context_data_match_common_player_count_i, context_data_match_common_has_bots_b, dt 
	from  ads_ww2.fact_mp_match_data_players 
-- Match Player Data Filter 
 
  where dt = date '{{DS_DATE_ADD(0)}}'
  and context_data_players_start_rank_i <=54
 
 ) b 
	
on a.dt = b.dt 
and a.context_headers_title_id_s = b.context_headers_title_id_s 
and a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 
and a.victim_user_id=b.context_data_players_client_user_id_l

join ( select distinct dt, context_headers_title_id_s, 
context_data_match_common_matchid_s, context_data_players_client_user_id_l, 
context_data_players_loadouts_index, division_l, name_s, inuse_b  
from ads_ww2.fact_mp_match_data_players_loadouts 
where dt= date '{{DS_DATE_ADD(0)}}'
) c 

on a.dt = c.dt 
and a.context_headers_title_id_s = c.context_headers_title_id_s 
and a.context_data_match_common_matchid_s = c.context_data_match_common_matchid_s 
and a.victim_user_id = c.context_data_players_client_user_id_l 
and a.victim_loadout_index_i = c.context_data_players_loadouts_index 

join loot_table d 
on a.victim_weapon_guid_l = d.loot_id 

-- Lives Data Filter

where (a.death_time_ms_i - a.spawn_time_ms_i) > 0 
--and (a.spawn_pos_ai[1] > 0 or a.spawn_pos_ai[2] > 0 or a.spawn_pos_ai[3] > 0) 
and a.means_of_death_s <> 'none' 

-- Loadout Data Filter

and c.inuse_b = TRUE 

-- Only Considering Custom loadouts 

and name_s like '%%custom%%'
) y 
group by 1,2,3,4,5,6,7,8,9
)

-- Get Number of Users Played with Each Division and the Weapon Class Used 

select  case when platform = '5597' then 'PC' 
       when platform = '5598' then 'XBOX' 
	   when platform = '5599' then 'PS4' end as Platform
,case when division_l = 0 then 'Infantry' 
       when division_l = 1 then 'Airborne' 
	   when division_l = 2 then 'Armoured' 
	   when division_l = 3 then 'Mountain' 
	   when division_l = 4 then 'Expeditionary' 
	   end as Division
, weapon_class,player_rank,player_prestige, count(distinct victim_user_id) as num_users, sum(num_spawns) as num_spawns, sum(duration_played) as duration_played, dt
from temp_div_lives 
group by 1,2,3,4,5,9
""" 
insert_division_usage_dashboard_task = qubole_operator('insert_division_usage_task',
                                              insert_division_usage_sql, 2, timedelta(seconds=600), dag) 

basic_training_script = '''
from pyspark.sql import SparkSession
spark = (SparkSession
            .builder
            .appName('Division Basic Training')
            .enableHiveSupport()
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .getOrCreate()
            )
from pyspark.sql import * 
from pyspark.sql import functions as F 
from pyspark.sql.functions import *
import sys
import time 
import datetime
from datetime import timedelta as td 
current_datetime = (datetime.datetime.now()).date()
print(current_datetime)
start_date = '{{DS_DATE_ADD(0)}}'
stop_date = '{{DS_DATE_ADD(0)}}'

date_to_start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
date_to_stop = datetime.datetime.strptime(stop_date, "%Y-%m-%d")
stats_date = date_to_start
max_date = date_to_stop
print(stats_date)
print(max_date)
def stats_Writer(date_to_run):
    stats_date = date_to_run
    print(stats_date)

    lootrest_data_query = """select name, reference, description, rarity, 'Launch' as productionlevel, category, rarity_s, loot_id, loot_group , BaseWeaponReference as weapon_base,         (case when collectionid is not null then 1 else 0 end) as is_collectible
    from as_s2.loot_v4_ext a 
    where productionlevel in ('Gold', 'TU1', 'MTX1') 
    and category in ('weapon', 'perk') 
    group by 1,2,3,4,5,6,7,8,9,10,11"""
    
    lootrest_data_df = spark.sql(lootrest_data_query).where(~upper(col("reference")).like("%ENLISTED%") & ~upper(col("reference")).like("%EXPERT%") & ~upper(col("reference")).like("%MASTER%")) 
    lootrest_data_df.createOrReplaceTempView("lootrest_data") 
    
    loadouts_data_query = """select dt, context_headers_title_id_s, context_data_match_common_matchid_s, context_data_players_client_user_id_l, context_data_players_loadouts_index,         division_l, name_s, explode(perkslots_al) as basic_trainings from ads_ww2.fact_mp_match_data_players_loadouts where dt = cast('%s' as date) and inuse_b = TRUE and name_s like '%%custom%%' """ %( stats_date )
    
    loadouts_data_df = spark.sql(loadouts_data_query)
    
    ## Filter the cases where Basic Traning = 0 
    loadouts_data_df=loadouts_data_df.filter(loadouts_data_df.basic_trainings != 0)
    
    lives_data_match_query = """select distinct b.context_headers_title_id_s
                                          , a.victim_weapon_s 
                                          , a.victim_weapon_guid_l 
                                          , a.attacker_weapon_s 
                                          , a.context_headers_event_id_s 
                                          , a.context_data_match_common_matchid_s 
                                          , a.victim_user_id 
                                          , a.victim_loadout_index_i 
                                          , a.attacker_user_id 
                                          , a.spawn_time_ms_i 
                                          , a.death_time_ms_i 
                                          , a.duration_ms_i 
                                          , b.context_data_players_start_rank_i as player_rank 
                                          , b.context_data_players_prestige_i as player_prestige 
                                          , b.dt 
                               from 
                                   ( select context_headers_title_id_s 
                                           , victim_weapon_s 
                                           , victim_weapon_guid_l 
                                           , attacker_weapon_s 
                                           , context_headers_event_id_s 
                                           , context_data_match_common_matchid_s 
                                           , victim_user_id 
                                           , victim_loadout_index_i 
                                           , attacker_user_id 
                                           , spawn_time_ms_i 
                                           , death_time_ms_i 
                                           , duration_ms_i 
                                           , means_of_death_s 
                                           , dt 
                                       from ads_ww2.fact_mp_match_data_lives 
                                       where dt =  cast('%s' as date)
                                       and death_time_ms_i - spawn_time_ms_i < 3600000 ) a 
                                       join ( select distinct context_headers_title_id_s 
                                           , context_data_match_common_matchid_s 
                                           , context_data_players_client_user_id_l 
                                           , context_data_players_prestige_i 
                                           , context_data_players_start_rank_i 
                                           , context_headers_event_id_s 
                                           , context_data_match_common_utc_start_time_i 
                                           , context_data_match_common_life_count_i 
                                           , context_data_match_common_player_count_i 
                                           , context_data_match_common_has_bots_b 
                                           , dt 
                                           from  ads_ww2.fact_mp_match_data_players 
                                           where dt = cast('%s' as date) 
                                           and context_data_players_start_rank_i between 0 and 54
                                           and context_data_players_prestige_i between 0 and 10
                                           and context_data_match_common_is_private_match_b  = false) b 
                                       ON a.dt = b.dt 
                                       AND a.context_headers_title_id_s = b.context_headers_title_id_s 
                                       AND a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 
                                       AND a.victim_user_id=b.context_data_players_client_user_id_l """ %( stats_date, stats_date)
    
    lives_data_match_df = spark.sql(lives_data_match_query)
    
    ## Create Merge AD 
    
    div_lives_match_df = lives_data_match_df.alias("lives").join(loadouts_data_df.alias("loadouts"), ((col("lives.context_headers_title_id_s") == col("loadouts.context_headers_title_id_s")) \
                                                           & (col("lives.context_data_match_common_matchid_s") == col("loadouts.context_data_match_common_matchid_s")) \
                                                           & (col("lives.victim_user_id") == col("loadouts.context_data_players_client_user_id_l"))), 'inner')\
                                                           .join(lootrest_data_df.alias("loot_d1"), (col("lives.victim_weapon_guid_l") == col("loot_d1.loot_id")), 'inner')\
                                                           .select(lives_data_match_df['*'], "loadouts.division_l", "loadouts.basic_trainings", "loot_d1.weapon_base", "loot_d1.loot_group") 
    
    agg_div_lives_match_df = (div_lives_match_df.groupBy("dt", "context_headers_title_id_s", "context_data_match_common_matchid_s", "victim_user_id", "weapon_base", "loot_group", "division_l", "basic_trainings", "player_rank", "player_prestige")\
                                              .agg(count("victim_user_id").alias("num_spawns")
                                              , F.sum((div_lives_match_df.death_time_ms_i - div_lives_match_df.spawn_time_ms_i)/1000.0 ).alias("duration_played")))\
                                              .groupBy("dt", "context_headers_title_id_s", "division_l", "basic_trainings", "player_rank", "player_prestige")\
                                              .agg(F.countDistinct("victim_user_id").alias("num_users")
                                              , F.sum("num_spawns").alias("num_spawns")
                                              , F.sum("duration_played").alias("duration_played"))
    
    agg_div_lives_match_df.createOrReplaceTempView("division_usage_table")
    
    ## Write Query to insert data into the Final table 
    
    insert_query = """ INSERT OVERWRITE TABLE as_s2.s2_division_basic_training_usage partition (raw_date) SELECT a.context_headers_title_id_s, a.division_l, a.basic_trainings, 'Weapon Class' as loot_group, a.player_rank, a.player_prestige, a.num_users, a.num_spawns, a.duration_played, b.reference, a.dt FROM division_usage_table a INNER JOIN lootrest_data b ON cast(a.basic_trainings as bigint) = b.loot_id 
    ) 
    AND player_rank between 0 and 54  
    AND player_prestige betwen 0 and 11 """
    
    ## Final data Insert 
    
    spark.sql(insert_query)
    
    ## Unpersist all the tables
    
    lootrest_data_df.unpersist()
    loadouts_data_df.unpersist()
    lives_data_match_df.unpersist()
    div_lives_match_df.unpersist()
    agg_div_lives_match_df.unpersist()

## Call the UDF
while date_to_start <= date_to_stop:
    stats_date = date_to_start
    date_to_start = date_to_start + td(days=1)
    print(stats_date)
    stats_Writer(stats_date)
spark.stop()'''

basic_training_spark_task = spark_operator('spark', 'basic_training_spark_task', basic_training_script, 'python', '--conf spark.driver.extraJavaOptions=-Djava.net.preferIPv4Stack=true --conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native --conf spark.eventLog.compress=true --conf spark.eventLog.enabled=true --conf spark.executor.cores=5 --conf spark.executor.extraJavaOptions=-Djava.net.preferIPv4Stack=true --conf spark.executor.instances=12 --conf spark.executor.memory=37g --conf spark.logConf=true --conf spark.scheduler.listenerbus.eventqueue.size=20000 --conf spark.speculation=false --conf spark.sql.qubole.split.computation=false --conf spark.ui.retainedJobs=33 --conf spark.ui.retainedStages=100 --conf spark.yarn.executor.memoryOverhead=2581 --conf spark.yarn.maxAppAttempts=1 --conf spark.dynamicAllocation.enabled=true --conf spark.master=yarn --conf spark.shuffle.service.enabled=true --conf spark.submit.deployMode=client', 2, timedelta(seconds=600), dag)

# Wire up the DAG , Setting Dependency of the tasks
insert_division_usage_dashboard_task.set_upstream(start_time_task)
basic_training_spark_task.set_upstream(start_time_task)
