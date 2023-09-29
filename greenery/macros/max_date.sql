{% macro max_date(model, column_name) %}


   select MAX({{ column_name }})
   from {{ model }}


{% endmacro %}