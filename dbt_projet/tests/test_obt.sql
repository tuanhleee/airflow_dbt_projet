{{config(severity="warn")}}

select 1
from
{{ref('obt_b')}} as obt
where obt.order_id IS NULL
OR obt.customer_id IS NULL
OR obt.order_item_id IS NULL
OR obt.product_id IS NULL
OR obt.store_id IS NULL
OR obt.employee_id IS NULL