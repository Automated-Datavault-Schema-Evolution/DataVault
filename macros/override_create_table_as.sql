{% macro spark__create_table_as(temporary, relation, sql) %}
  {% if temporary %}
    create temporary view {{ relation }} as
    {{ sql }}
  {% else %}
    create table if not exists {{ relation }}
    using delta
    as
    {{ sql }}
  {% endif %}
{% endmacro %}

{% macro spark__create_or_replace_table_as(relation, sql) %}
  -- Force CTAS instead of REPLACE to avoid V2 truncate capability checks
  create table if not exists {{ relation }}
  using delta
  as
  {{ sql }}
{% endmacro %}