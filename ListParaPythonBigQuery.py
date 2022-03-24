
from google.colab import auth
auth.authenticate_user()
from google.cloud import bigquery
from timeit import default_timer as timer
from datetime import timedelta,datetime
import pandas as pd
import sys


AuthorList=[
            {"usertype":"Subscriber"}
            ]
           
DateRangeList=[
               {"starttime":"2017-05-21","stoptime":"2017-05-21"} 
               ]
				
def run_query(query,starttime,stoptime,usertype,n=1):
  client = bigquery.Client(project='')
  for iter in range(n):
    query_job=client.query(query,bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("starttime","DATE",datetime.strptime(starttime,"%Y-%m-%d").date()),
            bigquery.ScalarQueryParameter("stoptime","DATE",datetime.strptime(stoptime,"%Y-%m-%d").date()),
            bigquery.ScalarQueryParameter("usertype","STRING",usertype)                                                         
    ],use_query_cache=False))
    df=query_job.result().to_dataframe()
    print(df)
    return [starttime,stoptime,usertype]				

homepage_part_1="""select @starttime as starttime,@stoptime as stoptime,@usertype as usertype from `bigquery-public-data.new_york_citibike.citibike_trips` limit 100"""	

query_run=[]
for author in AuthorList:
  for daterange in DateRangeList:
    query_run.append(run_query(query=homepage_part_1,
              starttime=daterange["starttime"],
              stoptime=daterange["stoptime"],
              usertype=author["usertype"]))
			  
col_list=["starttime","stoptime","usertype"]
import pandas as pd
aa= pd.DataFrame(query_run,columns=col_list)
aa