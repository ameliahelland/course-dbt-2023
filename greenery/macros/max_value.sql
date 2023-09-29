{% macro max_value(model, column_name) %}


   select MAX({{ column_name }})
   from {{ model }}


{% endmacro %}