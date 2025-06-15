/*
 * Copyright (c) Business Thinking Ltd. 2019-2025
 * This software includes code developed by the AutomateDV (f.k.a dbtvault) Team at Business Thinking Ltd. Trading as Datavault
 */

{%- macro stage(include_source_columns=none, source_model=none, hashed_columns=none, derived_columns=none, prejoined_columns=none, null_columns=none, ranked_columns=none) -%}

    {%- if include_source_columns is none -%}
        {%- set include_source_columns = true -%}
    {%- endif -%}

    {{- automate_dv.prepend_generated_by() }}

    {{ adapter.dispatch('stage', 'automate_dv')(include_source_columns=include_source_columns,
                                                source_model=source_model,
                                                hashed_columns=hashed_columns,
                                                derived_columns=derived_columns,
                                                prejoined_columns=prejoined_columns,
                                                null_columns=null_columns,
                                                ranked_columns=ranked_columns) -}}
{%- endmacro -%}

{%- macro default__stage(include_source_columns, source_model, hashed_columns, derived_columns, prejoined_columns, null_columns, ranked_columns) -%}

{% if (source_model is none) and execute %}

    {%- set error_message -%}
    Staging error: Missing source_model configuration. A source model name must be provided.
    e.g.
    [REF STYLE]
    source_model: model_name
    OR
    [SOURCES STYLE]
    source_model:
        source_name: source_table_name
    {%- endset -%}

    {{- exceptions.raise_compiler_error(error_message) -}}
{%- endif -%}

{#- Check for source format or ref format and create
    relation object from source_model -#}
{% if source_model is mapping and source_model is not none -%}

    {%- set source_name = source_model | first -%}
    {%- set source_table_name = source_model[source_name] -%}

    {%- set source_relation = source(source_name, source_table_name) -%}
    {%- set all_source_columns = automate_dv.source_columns(source_relation=source_relation) -%}
{%- elif source_model is not mapping and source_model is not none -%}

    {%- set source_relation = ref(source_model) -%}
    {%- set all_source_columns = automate_dv.source_columns(source_relation=source_relation) -%}
{%- else -%}

    {%- set all_source_columns = [] -%}
{%- endif -%}

{%- set columns_to_escape = automate_dv.process_columns_to_escape(derived_columns) | list -%}
{%- set derived_column_names = automate_dv.extract_column_names(derived_columns) | list -%}
{%- set null_column_names = automate_dv.extract_null_column_names(null_columns) | list -%}
{%- set hashed_column_names = automate_dv.extract_column_names(hashed_columns) | list -%}
{%- set ranked_column_names = automate_dv.extract_column_names(ranked_columns) | list -%}
{%- set exclude_column_names = derived_column_names + null_column_names + hashed_column_names | list -%}
{%- set source_and_derived_column_names = (all_source_columns + derived_column_names) | unique | list -%}
{% if not automate_dv.is_nothing(prejoined_columns) %}
    {%- set prejoined_columns = process_prejoined_columns(prejoined_columns) -%}
{%- endif -%}

{%- set source_columns_to_select = automate_dv.process_columns_to_select(all_source_columns, exclude_column_names) -%}
{%- set derived_columns_to_select = automate_dv.process_columns_to_select(source_and_derived_column_names, null_column_names + hashed_column_names) | unique | list -%}
{%- set derived_and_null_columns_to_select = automate_dv.process_columns_to_select(source_and_derived_column_names + null_column_names, hashed_column_names) | unique | list -%}
{%- set prejoined_columns_to_select = extract_prejoin_column_names(prejoined_columns) -%}
{%- set final_columns_to_select = [] -%}

{#- Include source columns in final column selection if true -#}
{%- if include_source_columns -%}
    {%- if automate_dv.is_nothing(derived_columns)
           and automate_dv.is_nothing(null_columns)
           and automate_dv.is_nothing(hashed_columns)
           and automate_dv.is_nothing(ranked_columns)
           and automate_dv.is_nothing(prejoined_columns) -%}
        {%- set final_columns_to_select = final_columns_to_select + all_source_columns -%}
    {%- else -%}
        {#- Only include non-overriden columns if not just source columns -#}
        {%- set final_columns_to_select = final_columns_to_select + source_columns_to_select + prejoined_columns_to_select -%}
    {%- endif -%}
{%- endif %}

WITH source_data AS (

    SELECT

    {{- "\n\n    " ~ automate_dv.print_list(list_to_print=all_source_columns, columns_to_escape=columns_to_escape) if all_source_columns else " *" }}

    FROM {{ source_relation }}
    {%- set last_cte = "source_data" %}
)

{%- if automate_dv.is_something(prejoined_columns) %},
{# Prejoining Business Keys of other source objects for Link purposes #}
prejoined_columns AS (

  SELECT
  {{ automate_dv.prefix(columns=all_source_columns, prefix_str='lcte') | indent(4) }}

  {# Iterate over each prejoin, doing logic checks and generating the select-statements #}
  {%- for prejoin in prejoined_columns -%}
    {%- set prejoin_alias = 'pj_' + loop.index|string -%}

    {# If extract_columns and/or aliases are passed as string convert them to a list so they can be used as iterators later #}
    {%- if not automate_dv.is_list(prejoin['extract_columns'])-%}
      {%- do prejoin.update({'extract_columns': [prejoin['extract_columns']]}) -%}
    {%- endif -%}
    {%- if not automate_dv.is_list(prejoin['aliases']) and automate_dv.is_something(prejoin['aliases']) -%}
      {%- do prejoin.update({'aliases': [prejoin['aliases']]}) -%}
    {%- endif -%}

 {# If passed, make sure there are as many aliases as there are extract_columns, ensuring a 1:1 mapping #}
    {%- if automate_dv.is_something(prejoin['aliases']) -%}
      {%- if not prejoin['aliases']|length == prejoin['extract_columns']|length -%}
        {%- do exceptions.raise_compiler_error("Prejoin aliases must have the same length as extract_columns. Got "
              ~ prejoin['extract_columns']|length ~ " extract_column(s) and " ~ prejoin['aliases']|length ~ " aliase(s).") -%}
      {%- endif -%}
    {%- endif -%}

 {# Generate the columns for the SELECT-statement #}
    {%- for column in prejoin['extract_columns'] %}
        ,{{ prejoin_alias }}.{{ column }} {% if automate_dv.is_something(prejoin['aliases']) -%} AS {{ prejoin['aliases'][loop.index0] }} {% endif -%}
    {%- endfor -%}
  {%- endfor -%}

  FROM {{ last_cte }} lcte
 {# Iterate over prejoins and generate the join-statements #}
  {%- for prejoin in prejoined_columns -%}
    {%- if 'ref_model' in prejoin.keys() -%}
      {% set relation = ref(prejoin['ref_model']) -%}
    {%- elif 'source_name' in prejoin.keys() and 'source_table' in prejoin.keys() -%}
      {%- set relation = source(prejoin['source_name']|string, prejoin['source_table']) -%}
    {%- else -%}
      {%- set error_message -%}
      Prejoin error: Invalid target entity definition. Allowed are:
      e.g.
      [REF STYLE]
      extracted_column_alias:
        ref_model: model_name
        bk: extracted_column_name
        this_column_name: join_columns_in_this_model
        ref_column_name: join_columns_in_ref_model
      OR
      [SOURCES STYLE]
      extracted_column_alias:
        source_name: name_of_ref_source
        source_table: name_of_ref_table
        bk: extracted_column_name
        this_column_name: join_columns_in_this_model
        ref_column_name: join_columns_in_ref_model

      Got:
      {{ prejoin }}
      {%- endset -%}

      {%- do exceptions.raise_compiler_error(error_message) -%}
    {%- endif -%}

    {%- if 'operator' not in prejoin.keys() -%}
      {%- set operator = 'AND' -%}
    {%- else -%}
      {%- set operator = prejoin['operator'] -%}
    {%- endif -%}
      {%- set prejoin_alias = 'pj_' + loop.index|string %}

    {%- if 'join_type' not in prejoin.keys() -%}
        {%- set join_type = 'left' -%}
    {%- else -%}
        {%- set join_type = prejoin['join_type'] -%}
    {%- endif -%}

    {{ '\n' + join_type }} join {{ relation }} as {{ prejoin_alias }}
        on {{ multikey(columns=prejoin['this_column_name'], prefix=['lcte', prejoin_alias], condition='=', operator=operator, right_columns=prejoin['ref_column_name']) }}
            {%- if 'this_column_name_for_const' in prejoin.keys() -%}
                {{ '\n' }}and {{ multikey(columns=prejoin['this_column_name_for_const'], prefix=['lcte', none], condition='=', operator=operator, right_columns=prejoin['ref_const']) }}
            {%- endif -%}
            {%- if 'ref_column_name_for_const' in prejoin.keys() -%}
                {{ '\n' }}and {{ multikey(columns=prejoin['ref_column_name_for_const'], prefix=[prejoin_alias, none], condition='=', operator=operator, right_columns=prejoin['this_const']) }}
            {%- endif -%}
  {%- endfor -%}

  {% set last_cte = "prejoined_columns" -%}
  {%- set final_columns_to_select = final_columns_to_select + prejoined_columns_to_select %}
)
{%- endif -%}

{%- if automate_dv.is_something(derived_columns) -%},

derived_columns AS (

    SELECT

    {{ automate_dv.print_list(list_to_print=prejoined_columns_to_select, columns_to_escape=columns_to_escape) }} {{"," if automate_dv.is_something(prejoined_columns_to_select) else ""}}
    {{ automate_dv.derive_columns(source_relation=source_relation, columns=derived_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "derived_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + derived_column_names %}
)
{%- endif -%}

{% if automate_dv.is_something(null_columns) -%},

null_columns AS (

    SELECT

    {{ automate_dv.print_list(list_to_print=derived_columns_to_select, columns_to_escape=columns_to_escape) }}{{"," if automate_dv.is_something(derived_columns_to_select) else ""}}
    {{ automate_dv.print_list(list_to_print=prejoined_columns_to_select, columns_to_escape=columns_to_escape) }} {{"," if automate_dv.is_something(prejoined_columns_to_select) else ""}}
    {{ automate_dv.null_columns(source_relation=none, columns=null_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "null_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + null_column_names %}
)
{%- endif -%}


{% if automate_dv.is_something(hashed_columns) -%},

hashed_columns AS (

    SELECT

    {{ automate_dv.print_list(list_to_print=derived_and_null_columns_to_select, columns_to_escape=columns_to_escape) }},
    {{ automate_dv.print_list(list_to_print=prejoined_columns_to_select, columns_to_escape=columns_to_escape) }} {{"," if automate_dv.is_something(prejoined_columns_to_select) else ""}}
    {% set processed_hash_columns = automate_dv.process_hash_column_excludes(hashed_columns, all_source_columns) -%}
    {{- hash_columns(columns=processed_hash_columns, columns_to_escape=columns_to_escape) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "hashed_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + hashed_column_names %}
)
{%- endif -%}

{% if automate_dv.is_something(ranked_columns) -%},

ranked_columns AS (

    SELECT *,

    {{ automate_dv.rank_columns(columns=ranked_columns) | indent(4) if automate_dv.is_something(ranked_columns) }}

    FROM {{ last_cte }}
    {%- set last_cte = "ranked_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + ranked_column_names %}
)
{%- endif -%}

,

columns_to_select AS (

    SELECT

    {{ automate_dv.print_list(list_to_print=final_columns_to_select | unique | list, columns_to_escape=columns_to_escape) }}

    FROM {{ last_cte }}
)

SELECT * FROM columns_to_select

{%- endmacro -%}
