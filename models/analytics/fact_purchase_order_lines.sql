WITH Fact_purchase_order_lines__source AS (
  SELECT 
    Fact_purchase_order_lines.Purchase_Order_line_id
  , Fact_purchase_order_lines.Purchase_order_id
  , Fact_purchase_order_lines.Received_Outers
  , Fact_purchase_order_lines.Package_type_id
  , Fact_purchase_order_lines.Expected_Unit_Price_Per_Outer
  , Fact_purchase_order_lines.Last_Receipt_date
  , Fact_purchase_order_lines.Stock_Item_id
  , Fact_purchase_order_lines.is_order_line_finalized 
  FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines` as Fact_purchase_order_lines
)

, Fact_purchase_order_lines__recast as (
  SELECT 
    cast (Fact_purchase_order_lines.Purchase_Order_line_id as int ) as Purchase_Order_line_key 
  , cast (Fact_purchase_order_lines.Purchase_order_id as int ) as Purchase_order_key
  , cast (Fact_purchase_order_lines.Package_type_id as int ) as Package_type_key
  , cast (Fact_purchase_order_lines.Stock_Item_id  as int ) as product_key 
  , cast (Fact_purchase_order_lines.Expected_Unit_Price_Per_Outer as int ) as Expected_Unit_Price_Per_Outer 
  , cast (Fact_purchase_order_lines.Last_Receipt_date as date ) as Last_Receipt_date 
  , cast (Fact_purchase_order_lines.Received_Outers as int ) as Received_Outers 
  , cast (Fact_purchase_order_lines.is_order_line_finalized as boolean ) as Is_order_line_finalized 
  FROM Fact_purchase_order_lines__source AS Fact_purchase_order_lines
)
, Fact_purchase_order_lines__final as (
  SELECT 
  Purchase_Order_line_key
  , Purchase_order_key
  , Package_type_key
  , product_key
  , Expected_Unit_Price_Per_Outer
  , Last_Receipt_date
  , Received_Outers
  , case when Is_order_line_finalized is true then 'Is finalized'
         when Is_order_line_finalized is false then 'Is not finalized'
    else 'Undefined' end as Is_order_line_finalized
FROM Fact_purchase_order_lines__recast
)
SELECT DISTINCT
  fact_purchase_lines.Purchase_Order_line_key
  , fact_purchase_lines.Purchase_order_key
  , fact_purchase_order.Order_date
--  , dim_product.Product_name
--  , dim_product.Brand_name
--  , dim_product.Colour_name
--  , dim_product.Size
  , fact_purchase_order.Delivery_method_name
  , fact_purchase_order.Supplier_name
  , dim_package_type.Package_type_name
  , fact_purchase_order.Transaction_type_name
  , fact_purchase_order.Payment_method_name
  , fact_purchase_order.Contact_person_name
  , fact_purchase_order.Is_order_finalized
  , fact_purchase_lines.Is_order_line_finalized
  , fact_purchase_lines.Received_Outers
  , fact_purchase_lines.Expected_Unit_Price_Per_Outer
  , fact_purchase_order.Expected_Delivery_Date
  , fact_purchase_lines.Last_Receipt_date
  , fact_purchase_lines.Package_type_key
  , fact_purchase_lines.product_key
FROM Fact_purchase_order_lines__final as fact_purchase_lines
LEFT JOIN {{ref('stg_dim_package_type')}} as Dim_package_type
    ON fact_purchase_lines.package_type_key = Dim_package_type.package_type_key
--LEFT JOIN {{ref('dim_product')}} as dim_product
--    ON fact_purchase_lines.Product_key = dim_product.Product_key
LEFT JOIN {{ref('fact_purchase_order')}} as Fact_purchase_order
    ON fact_purchase_lines.Purchase_order_key = Fact_purchase_order.Purchase_order_key
