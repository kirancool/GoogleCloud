from google.colab import auth
auth.authenticate_user()
from google.cloud import bigquery
from timeit import default_timer as timer
from datetime import timedelta,datetime
import pandas as pd
import sys

starttime="2017-10-11" #@param{type:"date"}
stoptime="2016-02-24" #@param{type:"date"}
usertype="Subscriber" #@param ["Subscriber","Consumer"]

starttime=datetime.strptime(starttime,"%Y-%m-%d").date()
stoptime =datetime.strptime(stoptime,"%Y-%m-%d").date()

query_test1="""
SELECT
CURRENT_DATE() as currentdate,
CURRENT_TIME() as currnettime,
CURRENT_TIMESTAMP() as currentimestamp, 
CAST(@starttime AS date) as start_time,
CAST(@stoptime as date) as stop_time,
cast(starttime as STRING) as test,
count(distinct bikeid) as num_bikes,
CONCAT(@usertype,'-',gender) as CONCAT_test,
substr(gender,1,1) as gender,
substr(@usertype,1,3) as subscriber,
cast(concat(cast(extract(year from stoptime) as STRING),'-','01','-01') as date) as date,
date_diff(current_date(),cast(@stoptime as date),DAY) as day_stop_time_to_current_time
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
where gender='female'
group by starttime,bikeid,CONCAT_test,gender,subscriber,stoptime limit 100"""

query_test="""
SELECT @starttime as starttime,@ stoptime as stoptime,@usertype as usertype ,
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
where gender='female' limit 100"""

client = bigquery.Client(project='')
job_config=bigquery.QueryJobConfig(use_legacy_sql=False)
query_parameter=[
                 bigquery.ScalarQueryParameter("starttime","DATE",starttime),
                 bigquery.ScalarQueryParameter("stoptime","DATE",stoptime),
                 bigquery.ScalarQueryParameter("usertype","STRING",usertype)
                 ]
job_config.query_parameters=query_parameter                  
df=client.query(query_test1,job_config=job_config).result().to_dataframe()
df