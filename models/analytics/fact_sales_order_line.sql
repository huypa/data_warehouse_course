WITH fact_sales_order_line_source AS (
  SELECT 
    order_line_id
  , order_id 
  , stock_item_id 
  , quantity 
  , unit_price 
  , tax_rate
  , description 
  , Picking_Completed_When
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line_caculated as (
  SELECT 
    cast(order_line_id as int) as sales_order_line_key
  , cast(order_id as int) as sales_order_key
  , cast(stock_item_id as int) as product_key
  , cast(quantity as int) as quantity
  , cast(unit_price as numeric ) as unit_price
  , cast(quantity as int) * cast(unit_price as numeric) as gross_amount
  , cast(quantity as int) * cast(unit_price as numeric) * tax_rate/100 as tax_amount
  , cast(quantity as int) * cast(unit_price as numeric) * (1-tax_rate/100) as net_amount
  , cast(description as string ) as description 
  , cast(Picking_Completed_When as timestamp ) as Line_picking_completed_when 
  FROM fact_sales_order_line_source
)

SELECT DISTINCT
-- fact line
    fact_header.Order_date
  , dim_date.day_of_week_short		
  , fact_header.Expected_delivery_date
  , fact_header.Is_undersupply_backordered
  , fact_header.Salesperson_person_id
  , fact_header.Contact_person_id
  , fact_header.Customer_purchase_order_number
  , fact_line.Quantity
  , fact_line.Unit_price
  , fact_line.Gross_amount
  , fact_line.Net_amount
  , fact_line.Tax_amount
  , fact_line.Description
  , fact_line.Line_picking_completed_when
  , fact_header.Order_picking_completed_when
  --- dim customer
  , dim_customer.Is_on_credit_Hold
  , dim_customer.Account_opened_Date
  , dim_customer.Standard_discount_percentage
  , dim_customer.Customer_category_name
  , dim_customer.Buying_group_name
  , dim_customer.Delivery_method_name as Cus_delivery_method_name
  , dim_customer.Delivery_city_name as Cus_delivery_city_name
  , dim_customer.Delivery_province_name as Cus_delivery_province_name
  --- dim product 
  , dim_product.Product_name
  , dim_product.Brand_name
  , dim_product.Size
  , dim_product.Is_chiller_stock
  , dim_product.Bar_code
  , dim_product.Unit_package_type_name
  , dim_product.Outer_package_type_name
  , dim_product.Supplier_name
  , dim_product.Delivery_method_name as Supplier_delivery_method_name
  , dim_product.Delivery_city_name as Supplier_delivery_city_name
  , dim_product.Delivery_province_name as Supplier_delivery_province_name
  -- dim date trivia
  , dim_date.year_month					
  , dim_date.month					
  , dim_date.year					
  , dim_date.year_number					
  , dim_date.is_week_day_or_weekend		
  -- key
  , fact_line.Sales_order_line_key
  , fact_line.Sales_order_key
  , fact_line.Product_key
  , fact_header.Customer_key
  , coalesce(fact_header.picked_by_person_key,-1) as Picked_by_person_key
FROM fact_sales_order_line_caculated as fact_line
LEFT JOIN {{ref('stg_fact_sales_order')}} as fact_header
  ON fact_line.sales_order_key = fact_header.sales_order_key
LEFT JOIN {{ref('dim_customer')}} as dim_customer
  ON fact_header.customer_key = dim_customer.customer_key
LEFT JOIN {{ref('dim_product')}} as dim_product
  ON fact_line.Product_key = dim_product.Product_key
RIGHT JOIN {{ref('dim_date')}} as dim_date
  ON fact_header.order_date = dim_date.date
 
--where fact_header.order_date is not null 