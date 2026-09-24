{#
    Limits a model to rows on or after a start date, to save compute quota.
    - With --vars '{start_date: 2024-01-01}', keeps rows from that date (UTC).
    - Without it, dev and ci keep the last 14 days and prod keeps everything.
#}
{% macro start_date_filter(column_name) -%}
    {%- set start_date = var('start_date') -%}
    {%- if start_date -%}
        {{ column_name }} >= cast('{{ start_date }}' as timestamp)
    {%- elif target.name in ('dev', 'ci') -%}
        {{ column_name }} >= date_sub(current_date(), 14)
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}
