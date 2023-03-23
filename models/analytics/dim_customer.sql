with customer_info_source as (
  select  
  *
  from `vit-lam-data.wide_world_importers.sales__customers`
)
, customer_info_rename as (
select 
  cast(customer_id as int ) as customer_key
  , cast(customer_name as string ) as customer_name
  , cast(phone_number as string ) as phone_number
  , cast(bill_to_customer_id as int) as bill_to_customer_id
  , cast(account_opened_date as date ) as Account_Opened_Date
  , cast(standard_discount_percentage as numeric ) as Standard_Discount_Percentage
  , cast(customer_category_id as int ) as customer_category_key
  , cast(buying_group_id as int ) as buying_group_key
  , cast(Primary_Contact_Person_ID as int ) as primary_contact_person_key
  , cast(Alternate_Contact_Person_ID as int ) as alternate_contact_person_key
  , cast(is_statement_sent as boolean ) as is_statement_sent
  , cast(Delivery_Method_ID as int ) as delivery_method_key
  , cast(Delivery_City_ID as int ) as delivery_city_key
  , case when is_on_credit_hold is true then 'On credit hold'
         when is_on_credit_hold is false then "Not on credit hold"
         when is_on_credit_hold is null then "Undefine"
         else "Invalid" 
    end as is_on_credit_hold
from customer_info_source
)
select 
  dim_customer.Customer_key
  , dim_customer.Customer_name 
  , dim_customer.Phone_number
  , dim_customer.Bill_to_customer_id
  , dim_customer.Is_on_credit_hold  
  , dim_customer.Account_Opened_Date 
  , dim_customer.Standard_Discount_Percentage 
  , dim_customer.Customer_category_key
  , coalesce(dim_customer_category.customer_category_name,"Invalid") as Customer_category_name
  , dim_customer.Buying_group_key
  , coalesce(dim_buying_group.buying_group_name,"Invalid") as Buying_group_name
  , dim_customer.Is_statement_sent 
  , dim_delivery_method.Delivery_method_key 
  , dim_delivery_method.Delivery_method_name 
  , dim_city.city_key as Delivery_city_key 
  , dim_city.city_name as Delivery_city_name 
  , dim_city.province_name as Delivery_province_name
  , dim_city.countries_name as Delivery_countries_name
  , dim_people.person_key as Primary_contact_person_key
  , dim_people.full_name as Primary_contact_full_name
from customer_info_rename as dim_customer 
left join {{ ref('stg_dim_customer_category') }} as dim_customer_category 
  on dim_customer.customer_category_key = dim_customer_category.customer_category_key
left join {{ref('stg_dim_buying_group')}} as dim_buying_group 
  on dim_customer.buying_group_key = dim_buying_group.buying_group_key
left join {{ref('stg_dim_delivery_method')}} as dim_delivery_method on dim_delivery_method.delivery_method_key = dim_customer.delivery_method_key
left join {{ref('stg_dim_city')}} as dim_city on dim_city.city_key = dim_customer.delivery_city_key
left join {{ref('dim_person')}} as dim_people on dim_people.person_key = dim_customer.primary_contact_person_key