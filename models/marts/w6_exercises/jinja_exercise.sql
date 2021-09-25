{% set product_categories = ['coffee beans', 'merch', 'brewing supplies'] %}

select
  date_trunc(created_at, month) as date_month,
{% for product_category in product_categories %}
    sum(case when product_category = '{{ product_category }}' then product_price end) as {{ product_category | replace(" ", "_") }}_amount
  {% if not loop.last  %}
    ,
    {% else %}
  {% endif %}
{% endfor %}
-- you may have to `ref` a different model here, depending on what you've built previously
from {{ ref('products_sale') }}
group by 1