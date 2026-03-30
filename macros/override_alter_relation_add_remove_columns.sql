{% macro spark__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {# Defensive defaults #}
  {% set add_columns = add_columns or [] %}
  {% set remove_columns = remove_columns or [] %}

  {# No-op early: avoids dbt-spark macro paths that can return None and crash compilation #}
  {% if (add_columns | length) == 0 and (remove_columns | length) == 0 %}
    {{ return('') }}
  {% endif %}

  {# In this pipeline we only need “append new columns” behavior reliably.
     DROP COLUMN is intentionally not executed here (policy/SEF should block narrowing anyway),
     and Spark/Delta drop behavior varies by version. #}
  {% if (remove_columns | length) > 0 %}
    {% do log("spark__alter_relation_add_remove_columns: skipping remove_columns for " ~ relation, info=True) %}
  {% endif %}

  {% if (add_columns | length) > 0 %}
    {% set rendered = [] %}
    {% for c in add_columns %}
      {% if c is mapping %}
        {% set cname = c.get('name') %}
        {% set ctype = c.get('data_type') %}
      {% else %}
        {% set cname = c.name %}
        {% set ctype = c.data_type %}
      {% endif %}
      {% if cname and ctype %}
        {% do rendered.append(adapter.quote(cname) ~ ' ' ~ ctype) %}
      {% endif %}
    {% endfor %}

    {% if (rendered | length) > 0 %}
      {% call statement('alter_add_cols_' ~ relation.identifier, fetch_result=False, auto_begin=True) %}
        alter table {{ relation }} add columns ({{ rendered | join(', ') }})
      {% endcall %}
    {% endif %}
  {% endif %}

  {{ return('') }}
{% endmacro %}
