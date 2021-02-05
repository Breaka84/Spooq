from pyspark.sql import functions as f, types as T
from pyspark.sql.utils import AnalysisException
import sys
import json

from .transformer import Transformer
from spooq2.transformer import Exploder, Mapper


class Flattener(Transformer):
    """
    Flattens input DataFrame schema and applies it to the DataFrame.
    """

    def __init__(self, pretty_names=True, ignore_ambiguous_columns=True):
        super().__init__()
        self.pretty_names = pretty_names
        self.ignore_ambiguous_columns = ignore_ambiguous_columns

    def transform(self, input_df):
        exploded_df, preliminary_mapping = self._get_preliminary_mapping(input_df, input_df.schema.jsonValue(), [], [], [])
        fixed_mapping = self._convert_python_to_spark_data_types(preliminary_mapping)
        mapped_df = Mapper(mapping=fixed_mapping, ignore_ambiguous_columns=self.ignore_ambiguous_columns).transform(exploded_df)
        return mapped_df

    def _get_preliminary_mapping(self, input_df, json_schema, mapping, current_path, exploded_arrays):
        for field in json_schema["fields"]:
            self.logger.debug(json.dumps(field, indent=2))
            if self._field_is_atomic(field):
                self.logger.debug(f"Atomic Field found: {field['name']}")
                mapping = self._add_field_to_mapping(mapping, current_path, field)
            elif self._field_is_struct(field):
                self.logger.debug(f"Struct Field found: {field['name']}")
                struct_name = field["name"]
                new_path = current_path + [struct_name]
                input_df, mapping = self._get_preliminary_mapping(input_df=input_df, json_schema=field["type"], mapping=mapping, current_path=new_path, exploded_arrays=exploded_arrays)
            elif self._field_is_array(field):
                self.logger.debug(f"Array Field found: {field['name']}")
                pretty_field_name = field["name"]
                field_name = "_".join(current_path + [pretty_field_name])
                array_path = ".".join(current_path + [pretty_field_name])

                if array_path in exploded_arrays:
                    self.logger.debug(f"Skipping explosion of {field_name}, as it was already exploded")
                    continue
                else:
                    if self.pretty_names:
                        try:
                            input_df[f"{pretty_field_name}_exploded"]
                            # If no exception is thrown, then the name already taken and the full path will be used
                            exploded_elem_name = f"{field_name}_exploded"
                        except AnalysisException:
                            exploded_elem_name = f"{pretty_field_name}_exploded"
                    else:
                        exploded_elem_name = f"{field_name}_exploded"

                    self.logger.debug(f"Exploding {array_path} into {exploded_elem_name}")
                    exploded_df = Exploder(path_to_array=array_path, exploded_elem_name=exploded_elem_name).transform(input_df)
                    exploded_arrays.append(array_path)
                    return self._get_preliminary_mapping(input_df=exploded_df, json_schema=exploded_df.schema.jsonValue(), mapping=[], current_path=[], exploded_arrays=exploded_arrays)
        return (input_df, mapping)

    def _field_is_atomic(self, field):
        return isinstance(field["type"], str)

    def _add_field_to_mapping(self, mapping, current_path, field):
        short_field_name = field["name"]
        source_path_array = current_path + [short_field_name]
        source_path = ".".join(source_path_array)
        included_source_paths = [source_path for (_, source_path, _) in mapping]
        included_field_names = [field_name for (field_name, _, _) in mapping]

        self.logger.debug("mapping: " + str(mapping))
        self.logger.debug("short_field_name: " + str(short_field_name))
        self.logger.debug("source_path_array: " + str(source_path_array))
        self.logger.debug("source_path: " + str(source_path))
        self.logger.debug("included_source_paths: " + str(included_source_paths))
        self.logger.debug("included_field_names: " + str(included_field_names))

        if source_path in included_source_paths:
            return mapping

        if self.pretty_names:
            self.logger.debug("Prettifying Names...")
            field_name = short_field_name
            self.logger.debug(f"Check if Field Name is unused: {field_name}")
            if field_name in included_field_names:
                self.logger.debug("Pretty Field Name already taken")
                for source_path_element in reversed(source_path_array[:-1]):
                    field_name = "_".join([source_path_element, field_name])
                    self.logger.debug(f"Check if Field Name is unused: {field_name}")
                    if field_name not in included_field_names:
                        self.logger.debug(f"Found unused Pretty Field Name: {field_name}")
                        break
        else:
            field_name = "_".join(source_path_array)

        field_name = field_name.replace("_exploded", "")
        data_type = field["type"]
        column_mapping = mapping[:]
        additional_column_mapping = (field_name, source_path, data_type)
        self.logger.debug(f"Adding mapping: {str(additional_column_mapping)}")
        column_mapping.append(additional_column_mapping)
        return column_mapping

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
        return [(name, source, data_type_matrix[data_type]) for (name, source, data_type) in mapping]
