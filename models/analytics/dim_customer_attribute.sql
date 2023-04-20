WITH dim_customer_attribute_caculate AS (
select 
Customer_key
, sum(Gross_amount) as Lifetime_sales_amount
, count(distinct Sales_order_key) as lifetime_sales_orders
, sum(case when date_trunc(Order_date,month)='2016-04-01' then Gross_amount end ) as LM_sales_amount
, count(distinct case when date_trunc(Order_date,month)='2016-04-01' then Sales_order_key end ) as LM_sales_orders
FROM {{ref('fact_sales_order_line')}}   
group by 1
)
, dim_customer_attribute_with_percentile as (
select
  Customer_key
  , Lifetime_sales_amount
  , lifetime_sales_orders
  , LM_sales_amount
  , LM_sales_orders 
  , percent_rank() over (order by Lifetime_sales_amount asc) as Lifetime_sales_amount_percentile
  , percent_rank() over (order by LM_sales_amount asc) as LM_sales_amount_percentile
from dim_customer_attribute_caculate
)
, dim_customer_attribute_with_segment as (
select 
  Customer_key
  , Lifetime_sales_amount
  , case when Lifetime_sales_amount_percentile < 0.3 then 'Low'
         when Lifetime_sales_amount_percentile between 0.3 and 0.7 then 'Medium'
         when Lifetime_sales_amount_percentile > 0.7 then 'High'
         else 'Invalid' 
    end as lifetime_monetary_segment
  , lifetime_sales_orders
  , LM_sales_amount
  , case when LM_sales_amount_percentile < 0.3 then 'Low'
         when LM_sales_amount_percentile between 0.3 and 0.7 then 'Medium'
         when LM_sales_amount_percentile > 0.7 then 'High'
         else 'Invalid' 
    end as LM_monetary_segment
  , LM_sales_orders 
from dim_customer_attribute_with_percentile
) 
select 
  Customer_key
  , Lifetime_sales_amount
  , lifetime_monetary_segment
  , lifetime_sales_orders
  , LM_sales_amount
  , LM_monetary_segment
  , LM_sales_orders 
from dim_customer_attribute_with_segment





WITH dim_customer_attribute_caculate AS (
select 
date_trunc(order_date,month) as year_month
, Customer_key
, sum(Gross_amount) as Sales_amount
from learn-dbt-379208.wide_world_importers_dwh.fact_sales_order_line
--FROM {{ref('fact_sales_order_line')}}   
group by 1,2
)
, dim_customer_attribute_with_percentile as (
select
  year_month
  , Customer_key
  , Sales_amount
  , percent_rank() over (partition by year_month order by Sales_amount asc) as LM_sales_amount_percentile
  , lag(Sales_amount,1) over (partition by Customer_key order by year_month asc) as pre_sa
  , sum(Sales_amount) over (partition by Customer_key order by year_month asc rows between unbounded preceding and current row ) as Lifetime_sales_amount
  , sum(Sales_amount) over (partition by Customer_key order by year_month asc ) as Lifetime_sales_amount_2
from dim_customer_attribute_caculate
)
select * from dim_customer_attribute_with_percentile
order by year_month,Customer_key
/*
, dim_customer_attribute_with_segment as (
select 
  Customer_key
  , Lifetime_sales_amount
  , case when Lifetime_sales_amount_percentile < 0.3 then 'Low'
         when Lifetime_sales_amount_percentile between 0.3 and 0.7 then 'Medium'
         when Lifetime_sales_amount_percentile > 0.7 then 'High'
         else 'Invalid' 
    end as lifetime_monetary_segment
  , lifetime_sales_orders
  , LM_sales_amount
  , case when LM_sales_amount_percentile < 0.3 then 'Low'
         when LM_sales_amount_percentile between 0.3 and 0.7 then 'Medium'
         when LM_sales_amount_percentile > 0.7 then 'High'
         else 'Invalid' 
    end as LM_monetary_segment
  , LM_sales_orders 
from dim_customer_attribute_with_percentile
) 
select 
  Customer_key
  , Lifetime_sales_amount
  , lifetime_monetary_segment
  , lifetime_sales_orders
  , LM_sales_amount
  , LM_monetary_segment
  , LM_sales_orders 
from dim_customer_attribute_with_segment
*/
