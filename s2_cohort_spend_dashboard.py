import airflow
from datetime import timedelta, datetime, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor
from quboleWrapper import qubole_wrapper, export_to_rdms

query_type = 'prod_presto'

# Set expected runtime in seconds, setting to 0 is 7200 seconds
expected_runtime = 0

# The group that owns this DAG
owner = "Analytic Services"

default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': datetime(2017, 12, 16),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_spenders_cohort_definition_dashboard',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(7, 30),
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

		
insert_cohort_spend_sql = """ Insert overwrite table as_s2.cohort_revenue_dashboard 

with player_cohorts as	
(
-- Resttricting the final numbers for DAUS 

 SELECT network_id, context_headers_user_id_s,g_rev 
	, case when player_ntile <= 1 then 'Active Superwhale' 
			when player_ntile <= 20 then 'Active Whale' 
			when player_ntile <= 50 then 'Active Dolphin' 
			else 'Active Minnow' end as player_type 

	FROM 
	(
		SELECT a.network_id, a.context_headers_user_id_s, g_rev, ntile(100) over (order by g_rev desc) as player_ntile 
		FROM 
			( SELECT context_headers_user_id_s -- Aggregate Lifetime Spend
			, network_id 
			, SUM(price_usd) as g_rev 
			FROM  as_s2.ww2_mtx_consumables_staging 
			WHERE dt <= date '{{DS_DATE_ADD(0)}}'
			GROUP BY 1,2 
			) a 
		JOIN -- Filter the users who arre avtive in last 7 days 
		   (SELECT DISTINCT network_id, client_user_id_l 
			FROM ads_ww2.fact_session_data 
			WHERE dt between date '{{DS_DATE_ADD(-6)}}' AND date '{{DS_DATE_ADD(0)}}'
			) b 
		ON a.network_id = b.network_id 
		AND a.context_headers_user_id_s = cast(b.client_user_id_l as varchar)
		
		) 
)


select  player_type,avg(g_rev) as revenue_generated, date '{{DS_DATE_ADD(0)}}' as dt
from player_cohorts
group by 1,3""" 

insert_cohort_spend_task = qubole_operator('s2_cohort_spend_task', insert_cohort_spend_sql, 2, timedelta(seconds=600), dag) 


insert_cohort_lifetime_crates_codp_sql = '''Insert overwrite as_s2.s2_cohort_definition_crates_codp 
with player_cohorts as	
(
	select network_id, context_headers_title_id_s, context_headers_user_id_s,g_rev 
	, case when player_ntile <= 1 then 'Active Superwhale' 
			when player_ntile <= 20 then 'Active Whale' 
			when player_ntile <= 50 then 'Active Dolphin' 
			else 'Active Minnow' end as player_cohort 
	from 
	(
		select a.network_id, a.context_headers_user_id_s, b.context_headers_title_id_s, g_rev, ntile(100) over (order by g_rev desc) as player_ntile 
		  from 
			(
			SELECT context_headers_user_id_s
	  , network_id
	  , SUM(price_usd) as g_rev
	  FROM  as_s2.ww2_mtx_consumables_staging
			  where  dt <= date '{{DS_DATE_ADD(0)}}' 
	  GROUP BY 1,2 ) a 
				join ( 
				     select distinct network_id, client_user_id_l, context_headers_title_id_s 
					 from ads_ww2.fact_session_data 
					 WHERE dt between date '{{DS_DATE_ADD(-6)}}' AND date '{{DS_DATE_ADD(0)}}'
					 ) b 
				on a.network_id = b.network_id 
				and a.context_headers_user_id_s = cast(b.client_user_id_l as varchar)
	) 
),
--Derive the cod points spent
cod_spent as 
(select player_cohort, sum(cod_spent) as cod_spent 
FROM
   (
	select context_headers_title_id_s 
	, context_headers_user_id_s 
	, player_cohort 
	, sum(case when balance_new_l < balance_old_l then balance_old_l - balance_new_l end) as cod_spent 
	from 
		(
		select a.*, b.player_cohort from ads_ww2.fact_currency_balances_view a 
		join player_cohorts b 
             on a.context_headers_title_id_s = b.context_headers_title_id_s 
             and a.context_headers_user_id_s = b.context_headers_user_id_s  
        where dt <= date '{{DS_DATE_ADD(0)}}' 
		and currency_id_l = 2 
		and balance_new_l < balance_old_l 
		)  
	group by 1,2,3 
    ) 
GROUP BY 1) ,
	
crates_opened as 
(
select player_cohort, (case when item_id_l=1 then 'MP Common'
when item_id_l = 2 then 'MP Rare'
when item_id_l = 5 then 'ZM Common' 
when item_id_l = 6 then 'ZM Rare'
when item_id_l = 75 then 'SD Winter'				
end)  as crate_type , count(distinct context_data_mmp_transaction_id_s) as num_crates  
FROM 
    (
     select distinct a.context_headers_title_id_s, a.context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
     , case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
     , case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
     , context_headers_timestamp_s, dt, b.player_cohort
     from ads_ww2.fact_mkt_consumeawards_data_userdatachanges_inventoryitems a 

     join player_cohorts b 

         on a.context_headers_title_id_s = b.context_headers_title_id_s 
         and a.context_headers_user_id_s = b.context_headers_user_id_s 

     where dt <= date '{{DS_DATE_ADD(0)}}' 
     and item_id_l in (1,2,5,6,75) 
     and quantity_new_l < quantity_old_l -- Filtering the records for crate opens
     ) 
  GROUP BY 1,2
)

--Aggregate number of crates opened by crate and cohort type

select a.player_cohort 
, a.avg_usd 
, b.cod_spent 
, round(coalesce(b.cod_spent,0)*pow(a.num_users, -1),2) as avg_cod_spent 
, c.crate_type 
, round(coalesce(c.num_crates,0)*pow(a.num_users,-1),2) as avg_num_crates 
, date '{{DS_DATE_ADD(0)}}' as raw_date
FROM 
(select player_cohort, avg(g_rev) as avg_usd, count(distinct row(context_headers_title_id_s, context_headers_user_id_s)) as num_users 
FROM player_cohorts 
GROUP BY 1) a 
JOIN cod_spent b 
on a.player_cohort = b.player_cohort 
LEFT JOIN crates_opened c 
ON a.player_cohort = c.player_cohort'''

insert_cohort_lifetime_crates_codp_task = qubole_operator('insert_cohort_lifetime_crates_codp_task', insert_cohort_lifetime_crates_codp_sql, 2, timedelta(seconds=600), dag) 

# Wire up the DAG , Setting Dependency of the tasks

insert_cohort_spend_task.set_upstream(start_time_task)
insert_cohort_lifetime_crates_codp_task.set_upstream(insert_cohort_spend_task)
