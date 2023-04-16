with fact_sale_order_line_source as (
  select 
    order_date
    , Salesperson_person_key
    , Net_amount
  from {{ref('fact_sales_order_line')}} 
) 
, fact_sale_order_line_recast as (
  select 
    date_trunc (order_date,month) as year_month
    , cast(Salesperson_person_key as int ) as salesperson_person_key
    , cast(Net_amount as numeric ) as Actual_revenue
  from fact_sale_order_line_source
)
, fact_revenue_by_sale_person_monthly as (
  select 
    year_month
    , salesperson_person_key
    , sum(Actual_revenue) as Actual_revenue
  from fact_sale_order_line_recast
  group by 1,2
)
, fact_revenue_by_sale_person_with_target_monthly as (
  select 
    coalesce(fact_actual.year_month,fact_target.year_month ) as year_month
    , fact_actual.salesperson_person_key
    , fact_actual.Actual_revenue
    , fact_target.Target_revenue
  from fact_revenue_by_sale_person_monthly fact_actual
  full outer join {{ref('stg_fact_sale_target_by_person')}} as fact_target 
    on fact_actual.year_month = fact_target.year_month 
    and fact_actual.salesperson_person_key = fact_target.salesperson_person_key
) 
, fact_revenue_by_sale_person_with_target_monthly_final as (
  select 
    year_month
    , salesperson_person_key
    , Actual_revenue
    , Target_revenue
    , round(Actual_revenue * 100 / Target_revenue , 1 ) as Achievement_pct
    , case when Actual_revenue / Target_revenue > 0.8 then 'Achieved'
           when Actual_revenue / Target_revenue < 0.8 then 'Not Achieved'
      else 'Invalid' 
      end as Is_achieved 
    , case when (Target_revenue is null or  salesperson_person_key is null ) then 'Invalid Saleperson Data'
      else 'Valid Saleperson Data'
      end as Is_valid_saleperson_data
  from fact_revenue_by_sale_person_with_target_monthly
)
select *
from fact_revenue_by_sale_person_with_target_monthly_final