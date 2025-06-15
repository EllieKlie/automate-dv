{%- macro extract_prejoin_column_names(prejoined_columns=none) -%}

    {%- set extracted_column_names = [] -%}

    {% if not automate_dv.is_something(prejoined_columns) %}
        {%- do return(extracted_column_names) -%}
    {% endif %}

    {% for prejoin in prejoined_columns %}
        {% if automate_dv.is_list(prejoin['aliases']) %}
            {% for alias in prejoin['aliases'] %}
                {%- do extracted_column_names.append(alias) -%}
            {% endfor %}
        {% elif automate_dv.is_something(prejoin['aliases']) %}
            {%- do extracted_column_names.append(prejoin['aliases']) -%}
        {% elif automate_dv.is_list(prejoin['extract_columns']) %}
            {% for column in prejoin['extract_columns'] %}
                {%- do extracted_column_names.append(column) -%}
            {% endfor %}
        {% else %}
            {%- do extracted_column_names.append(prejoin['extract_columns']) -%}
        {% endif %}
    {%- endfor -%}

    {%- do return(extracted_column_names) -%}

{%- endmacro -%}