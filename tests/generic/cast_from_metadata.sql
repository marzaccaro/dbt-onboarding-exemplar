{% test cast_from_metadata(model, metadata_table, metadata_schema='analytics') %}

{{ config(severity='warn') }}

{% set model_name = model.identifier %}


{% set query %}
    select column_name, expected_type
    from {{ metadata_schema }}.{{ metadata_table }}
    where lower(table_name) = lower('{{ model_name }}')
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
    {% set columns = results.rows %}
{% else %}
    {% set columns = [] %}
{% endif %}

{% if columns | length == 0 %}
    select null as validation_error where false
{% else %}
    select
        '{{ model }}' as model_name,
        *
        object_construct(
            {% for col_name, exp_type in columns %}
            '{{ col_name }}',
            case
              when {{ col_name }} is not null
               and try_cast({{ col_name }} as {{ exp_type }}) is null
              then {{ col_name }}
            end
            {% if not loop.last %},{% endif %}
            {% endfor %}
        ) as failed_columns
    from {{ model }}
    where false
    {% for col_name, exp_type in columns %}
        or ({{ col_name }} is not null and try_cast({{ col_name }} as {{ exp_type }}) is null)
    {% endfor %}
{% endif %}

{% endtest %}
