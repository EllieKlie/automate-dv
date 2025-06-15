/*
 * Copyright (c) Business Thinking Ltd. 2019-2024
 * This software includes code developed by the AutomateDV (f.k.a dbtvault) Team at Business Thinking Ltd. Trading as Datavault
 */

{%- macro multikey(columns, prefix=none, condition=none, operator='AND', right_columns=none) -%}

    {{- adapter.dispatch('multikey', 'automate_dv')(columns=columns, prefix=prefix, condition=condition, operator=operator, right_columns=right_columns) -}}

{%- endmacro %}

{%- macro default__multikey(columns, prefix=none, condition=none, operator='AND', right_columns=none) -%}

    {%- if prefix is string -%}
        {%- set prefix = [prefix] -%}
    {%- endif -%}

    {%- if columns is string -%}
        {%- set columns = [columns] -%}
    {%- endif -%}

    {%- if right_columns is none -%}
        {%- set right_columns = columns -%}
    {%- elif right_columns is string -%}
        {%- set right_columns = [right_columns] -%}
    {%- elif right_columns|length != columns|length -%}
        {%- set error_message -%}
      Multikey Error: If right_columns are defined, it must be the same length as columns.
      Got:
        Columns: {{ columns }} with length {{ columns|length }}
        right_columns: {{ right_columns }} with length {{ right_columns|length }}
        {%- endset -%}

        {{- exceptions.raise_compiler_error(error_message) -}}
    {%- endif -%}

    {%- if condition in ['<>', '!=', '='] -%}
        {%- for col in columns -%}
            {%- if prefix -%}
                {%- if prefix[1] -%}
                    {{- automate_dv.prefix([col], prefix[0], alias_target='target') }} {{ condition }} {{ automate_dv.prefix([right_columns[loop.index0]], prefix[1]) -}}
                {%- else -%}
                    {{- automate_dv.prefix([col], prefix[0], alias_target='target') }} {{ condition }} {{ right_columns[loop.index0] -}}
                {%- endif %}
            {%- endif %}
            {%- if not loop.last %} {{ operator }} {% endif -%}
        {% endfor -%}
    {%- else -%}
        {%- if automate_dv.is_list(columns) -%}
            {%- for col in columns -%}
                {{ (prefix[0] ~ '.') if prefix }}{{ col }} {{ condition if condition else '' }}
                {%- if not loop.last -%} {{ "\n    " ~ operator }} {% endif -%}
            {%- endfor -%}
        {%- else -%}
            {{ prefix[0] ~ '.' if prefix }}{{ columns }} {{ condition if condition else '' }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro -%}