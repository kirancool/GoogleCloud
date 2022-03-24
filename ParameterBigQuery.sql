age=72 #@param {type:"integer"}
job="admin." #@param ["admin.","developer"]
marital ="single" #@param ["single","married"]	
education	="secondary" #@param ["secondary", "tertiary"]
balance	="274" #@param {type:"string"}
month= "apr" #@param {type:"string"}
bq_param={
    "age":age,
    "job":job,
    "marital":marital,
    "education":education,	
    "balance":balance,	
    "month":month
}
or 

age = 46 #@param {type:"slider", min:10, max:80, step:1}
job="admin." #@param ["admin.","developer"]
marital ="single" #@param ["single","married"]	
education	="secondary" #@param ["secondary", "tertiary"]
balance	="274" #@param {type:"string"}
month= "apr" #@param {type:"string"}
bq_param={
    "age":age,
    "job":job,
    "marital":marital,
    "education":education,	
    "balance":balance,	
    "month":month
}


%%bigquery --project nice-forge-338515 df_test --params {bq_param}

select @age as age ,@job as job,@marital as marital, @education as education,
case when balance>=100 or balance<=1000 then 'low balance'
     when balance>=1001 or balance <=3000 then 'mid balance'
     when balance>=3001 or balance <=10000 then 'good balance'
     end balace,
     @month as month 
from `nice-forge-338515.Bank_marketing.bank` 
where day=extract(DAY from current_Date("GMT")) and 
( age=@age
 or 
 job=@job
 or 
 marital=@marital
 or 
 education =@education
 or 
 month=@month )
 and 
 (
   balance between 100 and 1000
   or
    balance between 1001 and 3000
   or 
   balance between 3001 and 10000
 )
 group by age,job,marital,education,balance,month;
 
select age as age ,job as job,marital as marital, education as education,
case when balance>=100 or balance<=1000 then 'low balance'
     when balance>=1001 or balance <=3000 then 'mid balance'
     when balance>=3001 or balance <=10000 then 'good balance'
     end balace,
     month as month,
extract(DAY from current_Date("GMT")-1) as day      
from `nice-forge-338515.Bank_marketing.bank` 
where day=extract(DAY from current_Date("GMT")) 
