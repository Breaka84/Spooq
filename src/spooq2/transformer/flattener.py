from pyspark.sql import functions as f, types as T
import sys
import json

from .transformer import Transformer
from spooq2.transformer import Exploder, Mapper


class Flattener(Transformer):
    """
    Flattens input DataFrame schema and applies it to the DataFrame.
    """

    def init(self, pretty_names=True):
        super().__init__()
        self.pretty_names = pretty_names

    def transform(self, input_df):
        exploded_df, preliminary_mapping = self._get_preliminary_mapping(input_df, input_df.schema.jsonValue(), [], [], [])
        fixed_mapping = self._convert_python_to_spark_data_types(preliminary_mapping)
        if self.pretty_names:
            fixed_mapping = self._prettify_column_names(fixed_mapping)
        mapped_df = Mapper(mapping=fixed_mapping).transform(exploded_df)
        return mapped_df

    def _get_preliminary_mapping(self, input_df, json_schema, mapping, current_path, exploded_arrays):
        for field in json_schema["fields"]:
            self.logger.debug(json.dumps(field, indent=2))
            if self._field_is_atomic(field):
                self.logger.debug(f"Atomic Field: {field['name']}")
                mapping = self._add_field_to_mapping(mapping, current_path, field)
            elif self._field_is_struct(field):
                self.logger.debug(f"Struct Field: {field['name']}")
                struct_name = field["name"]
                new_path = current_path + [struct_name]
                input_df, mapping = self._get_preliminary_mapping(input_df=input_df, json_schema=field["type"], mapping=mapping, current_path=new_path, exploded_arrays=exploded_arrays)
            elif self._field_is_array(field):
                self.logger.debug(f"Array Field: {field['name']}")
                array_name = field["name"]
                field_name = "_".join(current_path + [array_name])
                array_path = ".".join(current_path + [array_name])
                if array_path in exploded_arrays:
                    continue
                else:
                    exploded_df = Exploder(path_to_array=array_path, exploded_elem_name=f"{field_name}_exploded").transform(input_df)
                    exploded_arrays.append(array_path)
                    return self._get_preliminary_mapping(input_df=exploded_df, json_schema=exploded_df.schema.jsonValue(), mapping=[], current_path=[], exploded_arrays=exploded_arrays)
        return (input_df, mapping)

    def _field_is_atomic(self, field):
        return isinstance(field["type"], str)

    def _add_field_to_mapping(self, mapping, current_path, field):
        field_name = "_".join(current_path + [field["name"]])
        full_path = ".".join(current_path + [field["name"]])
        data_type = field["type"]
        column_mapping = (field_name, full_path, data_type)
        if column_mapping not in mapping:
            mapping.append(column_mapping)
        return mapping

    def _field_is_struct(self, field):
        field_type = field["type"]
        return (isinstance(field_type, dict) and
                len(field_type.get("fields", [])) > 0 and
                field_type.get("type", "") == "struct")

    def _field_is_array(self, field):
        field_type = field["type"]
        return (isinstance(field_type, dict) and
                "fields" not in field_type.keys() and
                field_type.get("type", "") == "array")

    def _convert_python_to_spark_data_types(self, mapping):
        data_type_matrix = {
            "long": "LongType",
            "int": "IntegerType",
            "string": "StringType",
            "double": "DoubleType",
            "float": "FloatType",
            "boolean": "BooleanType",
            "date": "DateType",
            "timestamp": "TimestampType"
        }
        try:
            return [(name, source, data_type_matrix[data_type]) for (name, source, data_type) in mapping]
        except Exception as e:
            import IPython; IPython.embed()

    def _prettify_column_names(self, mapping):

        return mapping
