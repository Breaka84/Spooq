from builtins import object
import pytest
from copy import deepcopy

from tests import DATA_FOLDER
from spooq import extractor as E
from spooq import transformer as T
from spooq import loader as L
from spooq.transformer.mapper import MissingColumnHandling
from spooq.transformer import mapper_transformations as spq
from spooq.pipeline import Pipeline


class TestPipeline(object):
    """Pipeline with an Extractor, a Transformers and a Loader"""

    @pytest.fixture()
    def extractor_params(self):
        return {"input_path": f"{DATA_FOLDER}/schema_v1/sequenceFiles"}

    @pytest.fixture()
    def transformer_params(self):
        # fmt: off
        return {"mapping": [
            ("id",                 "id",                       "int"),
            ("guid",               "guid",                     "string"),
            ("created_at",         "meta.created_at_sec",      spq.as_is(cast="long")),
            ("created_at_ms",      "meta.created_at_ms",       spq.as_is(cast="long")),
            ("version",            "meta.version",             "integer"),
            ("birthday",           "birthday",                 "timestamp"),
            ("location_struct",    "location",                 spq.as_is),
            ("latitude",           "location.latitude",        "double"),
            ("longitude",          "location.longitude",       "double"),
            ("birthday_str",       "attributes.birthday",      "string"),
            ("email",              "attributes.email",         "string"),
            ("myspace",            "attributes.myspace",       "string"),
            ("first_name",         "attributes.first_name",    "boolean"),
            ("last_name",          "attributes.last_name",     "boolean"),
            ("gender",             "attributes.gender",        "string"),
            ("ip_address",         "attributes.ip_address",    "string"),
            ("university",         "attributes.university",    "string"),
            ("friends",            "attributes.friends",       spq.as_is),
            ("friends_json",       "attributes.friends",       spq.to_json_string),
        ],
            "missing_column_handling": MissingColumnHandling.nullify
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
