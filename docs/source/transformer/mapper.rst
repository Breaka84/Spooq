Mapper
======

Class
-----

.. autoclass:: spooq.transformer.mapper.Mapper

Custom Transformations
------------------------------------------------

.. automodule:: spooq.transformer.mapper_transformations
    :no-members:

.. autosummary::
   :toctree: transformations

   spooq.transformer.mapper_transformations.as_is
   spooq.transformer.mapper_transformations.to_num
   spooq.transformer.mapper_transformations.to_bool
   spooq.transformer.mapper_transformations.to_timestamp
   spooq.transformer.mapper_transformations.str_to_array
   spooq.transformer.mapper_transformations.map_values
   spooq.transformer.mapper_transformations.meters_to_cm
   spooq.transformer.mapper_transformations.has_value
   spooq.transformer.mapper_transformations.apply
   spooq.transformer.mapper_transformations.to_json_string
   spooq.transformer.mapper_transformations.to_str
   spooq.transformer.mapper_transformations.to_int
   spooq.transformer.mapper_transformations.to_long
   spooq.transformer.mapper_transformations.to_float
   spooq.transformer.mapper_transformations.to_double


Custom Mapping Functions as Strings [DEPRECATED]
------------------------------------------------

.. automodule:: spooq.transformer.mapper_custom_data_types
    :no-members:

.. autosummary::
   :toctree: custom_transformations_as_string

   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_IntBoolean
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_IntNull
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_StringBoolean
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_StringNull
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_TimestampMonth
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_as_is
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_to_boolean
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_to_date
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_to_double
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_to_float
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_to_int
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_to_long
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_to_timestamp
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_unix_timestamp_ms_to_date
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_extended_string_unix_timestamp_ms_to_timestamp
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_has_value
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_json_string
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_keep
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_meters_to_cm
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_no_change
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_timestamp_ms_to_ms
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_timestamp_ms_to_s
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_timestamp_s_to_ms
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_timestamp_s_to_s
   spooq.transformer.mapper_custom_data_types._generate_select_expression_for_unix_timestamp_ms_to_spark_timestamp
   spooq.transformer.mapper_custom_data_types._generate_select_expression_without_casting
