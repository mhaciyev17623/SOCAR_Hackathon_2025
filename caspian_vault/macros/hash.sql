{% macro hk(value) -%}
    md5(cast({{ value }} as varchar))
{%- endmacro %}

{% macro hk2(v1, v2) -%}
    md5(cast({{ v1 }} as varchar) || '|' || cast({{ v2 }} as varchar))
{%- endmacro %}

{% macro hashdiff(cols) -%}
    md5({{ cols | join(" || '|' || ") }})
{%- endmacro %}
