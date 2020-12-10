from builtins import object
import pytest
from copy import deepcopy

from spooq2 import extractor as E
from spooq2 import transformer as T
from spooq2 import loader as L
from spooq2.pipeline import Pipeline


class TestPipeline(object):
    """Pipeline with an Extractor, a Transformers and a Loader"""

    @pytest.fixture()
    def extractor_params(self):
        return {"input_path": "data/schema_v1/sequenceFiles"}

    @pytest.fixture()
    def transformer_params(self):
        # fmt: off
        return {"mapping": [
            ("id",                 "id",                       "IntegerType"),
            ("guid",               "guid",                     "StringType()"),
            ("created_at",         "meta.created_at_sec",      "timestamp_s_to_s"),
            ("created_at_ms",      "meta.created_at_ms",       "timestamp_ms_to_ms"),
            ("version",            "meta.version",             "IntegerType()"),
            ("birthday",           "birthday",                 "TimestampType"),
            ("location_struct",    "location",                 "as_is"),
            ("latitude",           "location.latitude",        "DoubleType"),
            ("longitude",          "location.longitude",       "DoubleType"),
            ("birthday_str",       "attributes.birthday",      "StringType"),
            ("email",              "attributes.email",         "StringType"),
            ("myspace",            "attributes.myspace",       "StringType"),
            ("first_name",         "attributes.first_name",    "StringBoolean"),
            ("last_name",          "attributes.last_name",     "StringBoolean"),
            ("gender",             "attributes.gender",        "StringType"),
            ("ip_address",         "attributes.ip_address",    "StringType"),
            ("university",         "attributes.university",    "StringType"),
            ("friends",            "attributes.friends",       "no_change"),
            ("friends_json",       "attributes.friends",       "json_string"),
        ],
            "ignore_missing_columns": True
        }
        # fmt: on

    @pytest.fixture()
    def loader_params(self):
        return {
            "db_name": "test_pipeline",
            "table_name": "test_json_to_hive",
            "repartition_size": 2,
            "clear_partition": True,
            "auto_create_table": True,
            "overwrite_partition_value": True,
            "partition_definitions": [{"column_name": "dt", "column_type": "IntegerType", "default_value": 20191201}],
        }

    @pytest.fixture(scope="function")
    def prepare_database(self, spark_session, loader_params):
        spark_session.sql("create database if not exists {db}".format(db=loader_params["db_name"]))
        yield
        spark_session.sql("drop database if exists {db} cascade".format(db=loader_params["db_name"]))

    def test_json_sequence_file_to_hive_table(
        self, extractor_params, transformer_params, loader_params, spark_session, prepare_database
    ):
        """
        Extracting from JSON SequenceFile, Mapping and Loading to Hive Table
        """
        pipeline = Pipeline()
        pipeline.set_extractor(E.JSONExtractor(**extractor_params))
        pipeline.add_transformers(T.Mapper(**transformer_params))
        pipeline.set_loader(L.HiveLoader(**loader_params))
        pipeline.execute()

        loaded_df = spark_session.table(
            "{db}.{tbl}".format(db=loader_params["db_name"], tbl=loader_params["table_name"])
        )

        assert loaded_df.where(loaded_df.dt == 20191201).count() == 250
