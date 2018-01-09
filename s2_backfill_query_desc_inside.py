## Pupose of this dag is to work as backfill job if needed to utilize server

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
    'start_date': datetime(2017, 11, 03),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='backfill_dag',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(6, 30),
                             task_id='start_time_task',
                             dag=dag
                             )

current_date = (datetime.now()).date()
stats_date = current_date - timedelta(days=1)

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
## Spark Wrappper 

def spark_operator(label, task_id, program, language, arguments, retries, retry_delay, dag):
    return PythonOperator(
	    task_id=task_id,
		python_callable=spark_wrapper,
		provide_context=True,
		retries=retries,
		retry_delay=retry_delay,
		arguments=arguments,
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
		
insert_query_backfill_sql = """ Insert overwrite as_s2.s2_crates_balance_dashboard_cohort
with player_cohorts as	
(
	SELECT DISTINCT a.context_headers_title_id_s, a.network_id 
	, a.client_user_id_l 
	, case when a.dt < date('2017-11-21') then 'Active Non-Spender' else coalesce(b.player_type, 'Active Non-Spender') end as player_type 
	, a.dt 
	FROM ads_ww2.fact_session_data a 
	left JOIN 
	(select * from as_s2.s2_spenders_active_cohort_staging where raw_date = date '{{DS_DATE_ADD(0)}}') b 
	ON a.network_id = b.network_id 
	AND a.client_user_id_l = b.client_user_id 
	WHERE a.dt = date '{{DS_DATE_ADD(0)}}'
),

-- Combine All the inventory items table 

temp_inventory_items as
	(select a.*, b.player_type as player_type 
	FROM 
		(
select context_headers_title_id_s, context_headers_user_id_s, item_id_l 
, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_awardproduct_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_consumeawards_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s, 0, 0
 , context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_consumeinventoryitems_data_eventinfo_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_consumeinventoryitems_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
,context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_durableprocess_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 
--select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s, quantity_old_l, quantity_new_l,context_headers_timestamp_s, dt
--from ads_ww2.fact_mkt_durablerevoke_data_userdatachanges_inventoryitems 
--where dt <= date '{{DS_DATE_ADD(0)}}'
--union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_pawnitems_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_purchaseskus_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
) a 
join player_cohorts b 
ON a.context_headers_title_id_s = b.context_headers_title_id_s 
    and a.context_headers_user_id_s = cast(b.client_user_id_l as varchar)
	WHERE a.item_id_l in (1,2,5,6,75) 
)
	

select a.player_type, crate_type, sum(new_quantity) as no_of_crates 
, count(distinct row(context_headers_title_id_s, context_headers_user_id_s)) as num_users 
, b.total_users 
,  date '{{DS_DATE_ADD(0)}}' as dt
from 
(
select player_type, context_headers_user_id_s, context_headers_title_id_s 
, (case when item_id_l=1 then 'MP Common'
when item_id_l = 2 then 'MP Rare'
when item_id_l = 5 then 'ZM Common' 
when item_id_l = 6 then 'ZM Rare'
when item_id_l = 75 then 'SD Winter'				
end)  as crate_type 
,quantity_new_l as new_quantity 
,row_number() over (partition by context_headers_title_id_s, context_headers_user_id_s, item_id_l order by ts desc) as transact_rank
from 
(
select distinct dt, player_type  
,context_headers_title_id_s 
,context_headers_user_id_s 
,item_id_l
,quantity_old_l
,quantity_new_l
,dt
,to_unixtime(from_iso8601_timestamp(context_headers_timestamp_s)) as ts 
from temp_inventory_items  
)
) a 
-- Get latest transaction till that day 
left join 
(SELECT player_type, count(distinct row(client_user_id_l, context_headers_title_id_s)) as total_users 
FROM player_cohorts 
GROUP BY 1) b 
ON a.player_type = b.player_type 
where transact_rank = 1 
group by 1,2,5,6
""" 

insert_query_backfill_task = qubole_operator('insert_query_backfill',
                                              insert_query_backfill_sql, 3, timedelta(seconds=600), dag) 
test_script = '''from pyspark.sql import SparkSession
spark = (SparkSession
            .builder
            .appName('Double XP')
            .enableHiveSupport()
            .getOrCreate()
            )
from pyspark.sql import HiveContext

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")'''

test_spark_task = spark_operator('spark210', 'spark_test_script', test_script, 'PYTHON', '--conf spark.driver.extraJavaOptions=-Djava.net.preferIPv4Stack=true --conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native --conf spark.eventLog.compress=true --conf spark.eventLog.enabled=true --conf spark.executor.cores=5 --conf spark.executor.extraJavaOptions=-Djava.net.preferIPv4Stack=true --conf spark.executor.instances=12 --conf spark.executor.memory=37g --conf spark.logConf=true --conf spark.scheduler.listenerbus.eventqueue.size=20000 --conf spark.speculation=false --conf spark.sql.qubole.split.computation=false --conf spark.ui.retainedJobs=33 --conf spark.ui.retainedStages=100 --conf spark.yarn.executor.memoryOverhead=2581 --conf spark.yarn.maxAppAttempts=1 --conf spark.dynamicAllocation.enabled=true --conf spark.master=yarn --conf spark.shuffle.service.enabled=true --conf spark.submit.deployMode=client', 2, timedelta(seconds=600), dag)

# Wire up the DAG , Setting Dependency of the tasks
insert_query_backfill_task.set_upstream(start_time_task)
test_spark_task.set_upstream(start_time_task)
