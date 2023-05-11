select  
    category_level_1 as parent_category
    , category as child_category
    , min(Level) over (partition by category_level_1) as parent_level
    , level as child_level
from dim_category
where Category_level_1 <> 'Undefined'

 union all 

select  
    category_level_2 as parent_category
    , category as child_category
    , min(Level) over (partition by category_level_2) as parent_level
    , level as child_level
from dim_category
where Category_level_2 <> 'Undefined'

 union all 

select  
    category_level_3 as parent_category
    , category as child_category
    , min(Level) over (partition by category_level_3) as parent_level
    , level as child_level
from dim_category
where Category_level_3 <> 'Undefined'

 union all 

select  
    category_level_4 as parent_category
    , category as child_category
    , min(Level) over (partition by category_level_4) as parent_level
    , level as child_level
from dim_category
where Category_level_4 <> 'Undefined'

 union all 

select  
    category_level_5 as parent_category
    , category as child_category
    , min(Level) over (partition by category_level_5) as parent_level
    , level as child_level
from dim_category
where Category_level_5 <> 'Undefined'