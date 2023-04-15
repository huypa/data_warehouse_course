WITH Fact_supplier_transaction__source AS (
  SELECT 
  Fact_supplier_transaction.Supplier_Transaction_id
  , Fact_supplier_transaction.Transaction_type_id
  , Fact_supplier_transaction.Payment_method_id
  , Fact_supplier_transaction.Supplier_id
  , Fact_supplier_transaction.Purchase_order_id
  , Fact_supplier_transaction.Is_Finalized
  , Fact_supplier_transaction.Finalization_Date
  FROM `vit-lam-data.wide_world_importers.purchasing__supplier_transactions` as Fact_supplier_transaction
)

, Fact_supplier_transaction__caculated as (
  SELECT 
  cast( Fact_supplier_transaction.Supplier_Transaction_id as int ) as Supplier_Transaction_key
  , cast( Fact_supplier_transaction.Transaction_type_id as int ) as Transaction_type_key
  , cast( Fact_supplier_transaction.Payment_method_id as int ) as Payment_method_key
  , cast( Fact_supplier_transaction.Supplier_id as int ) as Supplier_key
  , cast( Fact_supplier_transaction.Purchase_order_id as int ) as Purchase_order_key
  , cast (Fact_supplier_transaction.Is_Finalized as boolean ) as Is_Finalized
  , cast (Fact_supplier_transaction.Finalization_Date as date ) as Finalization_Date
  FROM Fact_supplier_transaction__source AS Fact_supplier_transaction
)

SELECT DISTINCT
-- fact line
  fact_supplier.Supplier_Transaction_key
  , fact_supplier.Transaction_type_key
  , fact_supplier.Payment_method_key
  , fact_supplier.Supplier_key
  , fact_supplier.Purchase_order_key
  , fact_supplier.Is_Finalized
  , fact_supplier.Finalization_Date
  , dim_Supplier.Supplier_name
  , dim_transaction_types.Transaction_type_name
  , dim_payment_method.Payment_method_name
FROM Fact_supplier_transaction__caculated as fact_supplier
LEFT JOIN {{ref('stg_dim_payment_method')}} as dim_payment_method
    ON fact_supplier.Payment_method_key = dim_payment_method.Payment_method_key
LEFT JOIN {{ref('stg_dim_transaction_types')}} as dim_transaction_types
    ON fact_supplier.Transaction_type_key = dim_transaction_types.Transaction_type_key
LEFT JOIN {{ref('dim_supplier')}} as dim_supplier
    ON fact_supplier.Supplier_key = dim_supplier.Supplier_key
