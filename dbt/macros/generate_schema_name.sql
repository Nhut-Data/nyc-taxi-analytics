{#
    Override macro mặc định của dbt.

    Mặc định, dbt nối "{target.schema}_{custom_schema_name}" khi model có
    khai +schema riêng — gây ra tên dataset xấu/nhân đôi trên BigQuery
    (vd: nyc_taxi_marts_nyc_taxi_marts) vì target.schema trong profiles.yml
    cũng trùng với 1 trong các custom schema (nyc_taxi_marts).

    Fix: dùng CHÍNH XÁC custom_schema_name làm tên dataset, bỏ qua
    target.schema. Nếu model không khai +schema riêng, fallback về
    target.schema như bình thường.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}

        {{ target.schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}