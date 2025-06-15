{%- macro process_prejoined_columns(prejoined_columns=none) -%}
    {# Check if the old syntax is used for prejoined columns
        If so parse it to new list syntax #}

    {% if automate_dv.is_list(prejoined_columns) %}
        {% do return(prejoined_columns) %}
    {% else %}
        {% set output = [] %}
        {% for key, value in prejoined_columns.items() %}
            {% set ref_model = value.get('ref_model') %}
            {% set source_name = value.get('source_name') %}
            {% set source_table = value.get('source_table') %}
            {%- if 'operator' not in value.keys() -%}
                {%- do value.update({'operator': 'AND'}) -%}
                {%- set operator = 'AND' -%}
            {%- else -%}
                {%- set operator = value.get('operator') -%}
            {%- endif -%}
            {%- if 'join_type' not in value.keys() -%}
                {%- set join_type = 'left' -%}
            {%- else -%}
                {%- set join_type = value.get('join_type') -%}
            {%- endif -%}

            {% set this_column_name_for_const = [] %}
            {% set ref_const = [] %}
            {% set ref_column_name_for_const = [] %}
            {% set this_const = [] %}

            {%- if 'map_rule' in value.keys() -%}
                {%- if value.get('map_rule') is mapping -%}
                    {%- set map_rule = [value.get('map_rule')] -%}
                {%- else -%}
                    {%- set map_rule = value.get('map_rule') -%}
                {%- endif -%}
                {%- for rule in map_rule %}
                    {%- if 'this_column_name' in rule.keys() -%}
                        {% do this_column_name_for_const.append(rule.get('this_column_name')) %}
                        {% do ref_const.append(rule.get('ref_const')) %}
                    {%- endif -%}
                    {%- if 'ref_column_name' in rule.keys() -%}
                        {% do ref_column_name_for_const.append(rule.get('ref_column_name')) %}
                        {% do this_const.append(rule.get('this_const')) %}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}

    {% set match_criteria = (
            ref_model and output | selectattr('ref_model', 'equalto', ref_model) or
            source_name and output | selectattr('source_name', 'equalto', source_name) | selectattr('source_table', 'equalto', source_table)
        ) | selectattr('this_column_name', 'equalto', value.this_column_name)
        | selectattr('ref_column_name', 'equalto', value.ref_column_name)
        | selectattr('operator', 'equalto', value.operator) | selectattr('join_type', 'equalto', value.join_type)
        | list | first %}

            {% if match_criteria %}
                {% do match_criteria['extract_columns'].append(value.bk) %}
                {% do match_criteria['aliases'].append(key) %}
            {% else %}
                {% set new_item = {
                    'extract_columns': [value.bk],
                    'aliases': [key],
                    'this_column_name': value.this_column_name,
                    'ref_column_name': value.ref_column_name,
                    'operator': operator,
                    'join_type': join_type
                } %}

                {% if ref_model %}
                    {% do new_item.update({'ref_model': ref_model}) %}
                {% elif source_name and source_table %}
                    {% do new_item.update({'source_name': source_name, 'source_table': source_table}) %}
                {% endif %}

                {%- if this_column_name_for_const != [] -%}
                    {% do new_item.update({'this_column_name_for_const': this_column_name_for_const
                    , 'ref_const': ref_const}) %}
                {% endif %}
                {%- if ref_column_name_for_const != [] -%}
                    {% do new_item.update({'ref_column_name_for_const': ref_column_name_for_const
                    , 'this_const': this_const}) %}
                {% endif %}

                {% do output.append(new_item) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {%- do return(output) -%}

{%- endmacro -%}


