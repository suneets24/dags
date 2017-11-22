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
    'start_date': datetime(2017, 11, 8),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_currency_balance_dashboard',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(6, 30),
                             task_id='start_time_task',
                             dag=dag
                             )

current_date = (datetime.now()).date()
stats_date = current_date - timedelta(days=2)

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

		
insert_currency_balance_sql = """Insert overwrite table as_shared.s2_currency_balances 
with temp_currecny_balances as 
(
select distinct context_headers_title_id_s
, context_data_mmp_transaction_id_s
,context_headers_source_s
,context_headers_env_s
,context_headers_uuid_s
,context_headers__event_type_s
,context_headers_user_id_s
,context_headers_timestamp_s
,currency_id_l
,balance_new_l
,balance_old_l 
,dt
from ads_ww2.fact_mkt_purchaseskus_data_userdatachanges_currencybalances 
where dt <= date('%s')


union all 

select distinct context_headers_title_id_s
, context_data_mmp_transaction_id_s
,context_headers_source_s
,context_headers_env_s
,context_headers_uuid_s
,context_headers__event_type_s
,context_headers_user_id_s
,context_headers_timestamp_s
,currency_id_l
,balance_new_l
,balance_old_l
, dt 
from ads_ww2.fact_mkt_pawnitems_data_userdatachanges_currencybalances 
where dt <= date('%s')

union all 

select distinct context_headers_title_id_s
, context_data_mmp_transaction_id_s
,context_headers_source_s
,context_headers_env_s
,context_headers_uuid_s
,context_headers__event_type_s
,context_headers_user_id_s
,context_headers_timestamp_s
,currency_id_l
,balance_new_l
,balance_old_l 
, dt
from ads_ww2.fact_mkt_consumeawards_data_userdatachanges_currencybalances  
where dt <= date('%s')
) 


select currency_id, avg(currency_balance) as currency_balance, date('%s') as dt
from 
(
select context_headers_title_id_s, context_headers_user_id_s, case when currency_id_l = 1 then 'XP' 
                 when currency_id_l = 2 then 'COD Points'
				 when currency_id_l = 3 then 'Clan Token'
				 when currency_id_l = 4 then 'Prestige Token'
				 when currency_id_l = 5 then 'Supply Key'
				 when currency_id_l = 6 then 'Armory Credit'
				 when currency_id_l = 7 then 'Social XP'
				 when currency_id_l = 8 then 'Double XP'
				 when currency_id_l = 9 then 'Double Division XP'
				 when currency_id_l = 10 then 'Double Weapon XP'
				 when currency_id_l = 11 then 'Double Loot'
				 when currency_id_l = 12 then 'Double XP Daily Time' end as currency_id 
			 
	, sum(currency_change_per_day ) as currency_balance 
	from 

(
select dt, context_headers_title_id_s, context_headers_user_id_s, currency_id_l, sum(currency_gained)-sum(currency_spent) as currency_change_per_day 
from 
(
select dt, a.context_headers_title_id_s, a.context_headers_user_id_s, currency_id_l, balance_new_l , balance_old_l, context_headers__event_type_s
, context_headers_timestamp_s 
, to_unixtime(from_iso8601_timestamp(context_headers_timestamp_s))
, (case when balance_new_l > balance_old_l then (balance_new_l - balance_old_l )  else 0 end) as currency_gained 
, (case when balance_new_l < balance_old_l then (balance_old_l - balance_new_l )  else 0 end) as currency_spent 
from temp_currecny_balances a join 
(
select distinct context_headers_title_id_s, context_headers_user_id_s from 
temp_currecny_balances
where dt = date('%s')
--and context_data_match_common_is_private_match_b = False 
) c 
on a.context_headers_title_id_s = c.context_headers_title_id_s 
and a.context_headers_user_id_s = c.context_headers_user_id_s
) 
group by 1,2,3,4
)
group by 1,2,3 
)
where currency_id = 'Armory Credit'  
group by 1,3""" %(stats_date, stats_date, stats_date, stats_date, stats_date) 

insert_currency_balance_task = qubole_operator('daily_end_of_day_currency_balance',
                                              insert_currency_balance_sql, 2, timedelta(seconds=600), dag) 

# Wire up the DAG , Setting Dependency of the tasks
insert_currency_balance_task.set_upstream(start_time_task)
