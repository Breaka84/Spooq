from pyspark.sql import functions as F, types as T
from pyspark.sql.utils import AnalysisException
import sys
import json

from .transformer import Transformer
from spooq2.transformer import Exploder, Mapper


class Flattener(Transformer):
    """
    Flattens and explodes an input DataFrame.

    Example
    -------
    >>> import datetime
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Flattener
    >>>
    >>> input_df = spark.createDataFrame([Row(
    >>>     struct_val_1=Row(
    >>>         struct_val_2=Row(
    >>>             struct_val_3=Row(
    >>>                 struct_val_4=Row(int_val=4789),
    >>>                 long_val=478934243342334),
    >>>             string_val="Hello"),
    >>>         double_val=43.12),
    >>>     timestamp_val=datetime.datetime(2021, 1, 1, 12, 30, 15)
    >>> )])
    >>> input_df.printSchema()
    root
     |-- struct_val_1: struct (nullable = true)
     |    |-- struct_val_2: struct (nullable = true)
     |    |    |-- struct_val_3: struct (nullable = true)
     |    |    |    |-- struct_val_4: struct (nullable = true)
     |    |    |    |    |-- int_val: long (nullable = true)
     |    |    |    |-- long_val: long (nullable = true)
     |    |    |-- string_val: string (nullable = true)
     |    |-- double_val: double (nullable = true)
     |-- timestamp_val: timestamp (nullable = true)
    >>>
    >>> flat_df = Flattener().transform(input_df)
    [spooq2] 2021-02-19 15:47:59,921 INFO flattener::_explode_and_get_mapping::90: Exploding Input DataFrame and Generating Mapping (This can take some time depending on the complexity of the input DataFrame)
    [spooq2] 2021-02-19 15:48:01,870 INFO mapper::transform::117: Generating SQL Select-Expression for Mapping...
    [spooq2] 2021-02-19 15:48:01,942 INFO mapper::transform::143: SQL Select-Expression for new mapping generated!
    >>> flat_df.printSchema()
    root
     |-- int_val: long (nullable = true)
     |-- long_val: long (nullable = true)
     |-- string_val: string (nullable = true)
     |-- double_val: double (nullable = true)
     |-- timestamp_val: timestamp (nullable = true)
    >>>

    Parameters
    ----------
    pretty_names : :any:`bool`, Defaults to True
        Defines if Spooq should try to use the shortest name possible
        (starting from the deepest key / rightmost in the path)

    keep_original_columns : :any:`bool`, Defaults to False
        Whether the original columns should be kept under the ``original_columns`` struct next to the
	flattenend columns.

    convert_timestamps : :any:`bool`, Defaults to True
        Defines if Spooq should use special timestamp and datetime transformation on column names
        with specific suffixes (_at, _time, _date)

    ignore_ambiguous_columns : :any:`bool`, Defaults to True
        This flag is forwarded to the Mapper Transformer.
    """

    def __init__(self, pretty_names=True, keep_original_columns=False, convert_timestamps=True, ignore_ambiguous_columns=True):
        super().__init__()
        self.pretty_names = pretty_names
        self.keep_original_columns = keep_original_columns
        self.convert_timestamps = convert_timestamps
        self.ignore_ambiguous_columns = ignore_ambiguous_columns
        self.python_script = []

    def transform(self, input_df):
        exploded_df, mapping = self._explode_and_get_mapping(input_df)
        mapped_df = Mapper(mapping=mapping, ignore_ambiguous_columns=self.ignore_ambiguous_columns).transform(exploded_df)
        return mapped_df

    def get_script(self, input_df):
        """
        Flattens and explodes an input DataFrame but instead of directly applying it to the input DataFrame,
        it returns a script that contains all imports, explosion transformations, and the resulting mapping.

        Parameters
        ----------
        input_df : :py:class:`pyspark.sql.DataFrame`
            Input DataFrame

        Returns
        -------
        :any:`list` of :any:`str`
            List of code lines to execute the flattening steps explicitly.

        Hint
        ----
        See :py:mod:`spooq2.transformer.flattener.Flattener.export_script` for exporting the script to a file.
        """
        self._explode_and_get_mapping(input_df)
        return self.python_script

    def export_script(self, input_df, file_name):
        """
        Flattens and explodes an input DataFrame but instead of directly applying it to the input DataFrame,
        it exports a script that contains all imports, explosion transformations, and the resulting mapping
        to an external file.

        Parameters
        ----------
        input_df : :py:class:`pyspark.sql.DataFrame`
            Input DataFrame

        file_name : :any:`str`
            Name of file to which the script should be exported to.

        Attention
        ---------
        Currently, only local paths are possible as it uses Python's internal ``open()`` function.

        Hint
        ----
        See :py:mod:`spooq2.transformer.flattener.Flattener.get_script` for getting the script as a python object.
        """
        with open(file_name, "w") as file_handle:
            for line in self.get_script(input_df):
                file_handle.write(f"{line}\n")
        self.logger.info(f"Flattening Tranformation Script was exported to {file_name}")

    def _explode_and_get_mapping(self, input_df):
        self.logger.info("Exploding Input DataFrame and Generating Mapping (This can take some time depending on the complexity of the input DataFrame)")
        initial_mapping = []
        if self.keep_original_columns:
            input_df = input_df.withColumn("original_columns", F.struct(*input_df.columns))
        self._script_set_imports()
        self._script_add_input_statement(input_df)
        exploded_df, preliminary_mapping = self._get_preliminary_mapping(input_df, input_df.schema.jsonValue(), initial_mapping, [], [])
        fixed_mapping = self._convert_python_to_spark_data_types(preliminary_mapping)
        if self.keep_original_columns:
            fixed_mapping.insert(0, ("original_columns", "original_columns", "as_is"))
        self._script_apply_mapping(mapping=fixed_mapping)
        return exploded_df, fixed_mapping

    def _get_preliminary_mapping(self, input_df, json_schema, mapping, current_path, exploded_arrays):
        for field in json_schema["fields"]:
            self.logger.debug(json.dumps(field, indent=2))
            if self._field_is_original_columns_struct(field) and self.keep_original_columns:
                continue
            elif self._field_is_atomic(field):
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
                    self._script_add_explode_transformation(path_to_array=array_path, exploded_elem_name=exploded_elem_name)
                    exploded_df = Exploder(path_to_array=array_path, exploded_elem_name=exploded_elem_name, drop_rows_with_empty_array=False).transform(input_df)
                    exploded_arrays.append(array_path)
                    return self._get_preliminary_mapping(input_df=exploded_df, json_schema=exploded_df.schema.jsonValue(), mapping=[], current_path=[], exploded_arrays=exploded_arrays)
        return (input_df, mapping)

    def _field_is_original_columns_struct(self, field):
        return self._field_is_struct(field) and field["name"] == "original_columns"

    def _field_is_atomic(self, field):
        return isinstance(field["type"], str)

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

        fixed_mapping = [
            (name, source, data_type_matrix.get(data_type, data_type))
            for (name, source, data_type)
            in mapping
        ]

        if self.convert_timestamps:
            fixed_mapping_with_timestamps = []
            for (column_name, source_path, data_type) in fixed_mapping:
                if column_name.endswith(("_at", "_time")):
                    data_type = "extended_string_to_timestamp"
                elif column_name.endswith("_date"):
                    data_type = "extended_string_to_date"
                fixed_mapping_with_timestamps.append((column_name, source_path, data_type))
            fixed_mapping = fixed_mapping_with_timestamps

        return fixed_mapping

    def _script_set_imports(self):
        self.python_script = [
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F, types as T",
            "",
            "from spooq2.transformer import Mapper, Exploder",
            "",
            "spark = SparkSession.builder.getOrCreate()",
        ]
        self.logger.debug("\n".join(self.python_script))

    def _script_add_input_statement(self, input_df):
        input_file_names_string = ",".join([row.filename for row in input_df.select(F.input_file_name().alias("filename")).distinct().collect()])
        self.python_script.append(f"input_df = spark.read.load('{input_file_names_string}')")
        if self.keep_original_columns:
            self.python_script.append("input_df = input_df.withColumn('original_columns', F.struct(*input_df.columns))")
        self.python_script.append("")

        self.logger.debug("\n".join(self.python_script))

    def _script_add_explode_transformation(self, path_to_array, exploded_elem_name):
        self.python_script.append(f"input_df = Exploder('{path_to_array}', '{exploded_elem_name}', drop_rows_with_empty_array=False).transform(input_df)")
        self.logger.debug("\n".join(self.python_script))

    def _script_apply_mapping(self, mapping):
        string_lenghts = [[len(value) for value in mapping_line] for mapping_line in mapping]
        max_col_len, max_source_len, max_type_len = [int(max(column)) for column in [*zip(*string_lenghts)]]
        self.python_script.extend([
            "",
            "# fmt:off",
            "mapping_to_apply = [",
        ])
        for (column_name, source_path, data_type) in mapping:
            mapping_string = "    "
            mapping_string += f"('{column_name}',"
            whitespace_to_fill = max_col_len - len(column_name) + 1
            mapping_string += " " * whitespace_to_fill
            mapping_string += f"'{source_path}',"
            whitespace_to_fill = max_source_len - len(source_path) + 1
            mapping_string += " " * whitespace_to_fill
            mapping_string += f"'{data_type}'),"
            self.python_script.append(mapping_string)

        self.python_script.extend([
            "]",
            "# fmt:on",
            "",
            "output_df = Mapper(mapping_to_apply).transform(input_df)"
        ])

        self.logger.debug("\n".join(self.python_script))
