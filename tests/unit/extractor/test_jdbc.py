from __future__ import division
from builtins import str
from builtins import object
from past.utils import old_div
import pytest
import sqlite3
import pandas as pd
from doubles import allow

from spooq.extractor import JDBCExtractorIncremental


@pytest.fixture()
def default_params():
    return {
        "jdbc_options": {
            "url": "url",
            "driver": "driver",
            "user": "user",
            "password": "password",
        },
        "partition": 20180518,
        "source_table": "MOCK_DATA",
        "spooq_values_table": "test_spooq_values_tbl",
        "spooq_values_db": "test_spooq_values_db",
        "spooq_values_partition_column": "updated_at",
        "cache": True,
    }


@pytest.fixture()
def extractor(default_params):
    return JDBCExtractorIncremental(**default_params)


@pytest.fixture()
def spooq_values_pd_df(spark_session, default_params):
    # fmt: off
    input_data = {
        'partition_column': ['updated_at',          'updated_at',          'updated_at'],
        'dt':               [20180515,               20180516,              20180517],
        'first_value':      ['2018-01-01 03:30:00', '2018-05-16 03:30:00', '2018-05-17 03:30:00'],
        'last_value':       ['2018-05-16 03:29:59', '2018-05-17 03:29:59', '2018-05-18 03:29:59']
    }
    # fmt: on
    pd_df = pd.DataFrame(input_data)
    spark_session.conf.set("hive.exec.dynamic.partition", "true")
    spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark_session.sql("DROP DATABASE IF EXISTS {db} CASCADE".format(db=default_params["spooq_values_db"]))
    spark_session.sql("CREATE DATABASE {db}".format(db=default_params["spooq_values_db"]))
    spark_session.createDataFrame(pd_df).write.partitionBy("dt").saveAsTable(
        "{db}.{tbl}".format(db=default_params["spooq_values_db"], tbl=default_params["spooq_values_table"])
    )
    yield pd_df
    spark_session.sql("DROP DATABASE IF EXISTS {db} CASCADE".format(db=default_params["spooq_values_db"]))


class TestJDBCExtractorIncremental(object):
    """Testing of spooq.extractor.JDBCExtractorIncremental

    spooq_values_pd_df:

        partition_column        dt          first_value           last_value
    0         updated_at  20180515  2018-01-01 03:30:00  2018-05-16 03:29:59
    1         updated_at  20180516  2018-05-16 03:30:00  2018-05-17 03:29:59
    2         updated_at  20180517  2018-05-17 03:30:00  2018-05-18 03:29:59

    """

    class TestBasicAttributes(object):
        def test_logger_should_be_accessible(self, extractor):
            assert hasattr(extractor, "logger")

        def test_name_is_set(self, extractor):
            assert extractor.name == "JDBCExtractorIncremental"

        def test_str_representation_is_correct(self, extractor):
            assert str(extractor) == "Extractor Object of Class JDBCExtractorIncremental"

    class TestBoundaries(object):
        """Deriving boundaries from previous loads logs (spooq_values_pd_df)"""

        @pytest.mark.parametrize(
            ("partition", "value"),
            [(20180510, "2018-01-01 03:30:00"), (20180515, "2018-05-16 03:30:00"), (20180516, "2018-05-17 03:30:00")],
        )
        def test__get_lower_bound_from_succeeding_partition(self, partition, value, spooq_values_pd_df, extractor):
            """Getting the upper boundary partition to load"""
            assert extractor._get_lower_bound_from_succeeding_partition(spooq_values_pd_df, partition) == value

        @pytest.mark.parametrize(
            ("partition", "value"),
            [
                (20180516, "2018-05-16 03:29:59"),
                (20180517, "2018-05-17 03:29:59"),
                (20180518, "2018-05-18 03:29:59"),
                (20180520, "2018-05-18 03:29:59"),
            ],
        )
        def test__get_upper_bound_from_preceding_partition(self, partition, value, spooq_values_pd_df, extractor):
            """Getting the lower boundary partition to load"""
            assert extractor._get_upper_bound_from_preceding_partition(spooq_values_pd_df, partition) == value

        @pytest.mark.parametrize(
            ("partition", "boundaries"),
            [
                (20180515, ("2018-01-01 03:30:00", "2018-05-16 03:29:59")),
                (20180516, ("2018-05-16 03:30:00", "2018-05-17 03:29:59")),
                (20180517, ("2018-05-17 03:30:00", "2018-05-18 03:29:59")),
            ],
        )
        def test__get_lower_and_upper_bounds_from_current_partition(
            self, partition, boundaries, spooq_values_pd_df, extractor
        ):
            assert extractor._get_lower_and_upper_bounds_from_current_partition(
                spooq_values_pd_df, partition
            ) == tuple(boundaries)

        def test__get_previous_boundaries_table(self, extractor, spooq_values_pd_df):
            """Getting boundaries from previously loaded partitions"""
            try:
                del extractor.spooq_values_partition_column
            except AttributeError:
                pass
            assert not hasattr(extractor, "spooq_values_partition_column")
            df = extractor._get_previous_boundaries_table(extractor.spooq_values_db, extractor.spooq_values_table)
            assert 3 == df.count()
            assert extractor.spooq_values_partition_column == "updated_at"

        # fmt: off
        @pytest.mark.parametrize(
            ("partition", "boundaries"),
            [(20180510, (False,                 '2018-01-01 03:30:00')),
            (20180515,  ('2018-01-01 03:30:00', '2018-05-16 03:29:59')),
            (20180516,  ('2018-05-16 03:30:00', '2018-05-17 03:29:59')),
            (20180517,  ('2018-05-17 03:30:00', '2018-05-18 03:29:59')),
            (20180518,  ('2018-05-18 03:29:59', False)),
            (20180520,  ('2018-05-18 03:29:59', False))]
        )
        # fmt: on
        def test__get_boundaries_for_import(self, extractor, spooq_values_pd_df, partition, boundaries):
            assert extractor._get_boundaries_for_import(partition) == boundaries

    class TestQueryConstruction(object):
        """Constructing Query for Source Extraction with Boundaries in Where Clause"""

        @pytest.mark.parametrize(
            ("boundaries", "expected_query"),
            [
                ((False, False), "select * from MOCK_DATA"),
                ((False, 1024), "select * from MOCK_DATA where updated_at <= 1024"),
                ((False, "1024"), "select * from MOCK_DATA where updated_at <= 1024"),
                ((False, "g1024"), 'select * from MOCK_DATA where updated_at <= "g1024"'),
                (
                    (False, "2018-05-16 03:29:59"),
                    'select * from MOCK_DATA where updated_at <= "2018-05-16 03:29:59"',
                ),
                ((1024, False), "select * from MOCK_DATA where updated_at > 1024"),
                (("1024", False), "select * from MOCK_DATA where updated_at > 1024"),
                (("g1024", False), 'select * from MOCK_DATA where updated_at > "g1024"'),
                (
                    ("2018-05-16 03:29:59", False),
                    'select * from MOCK_DATA where updated_at > "2018-05-16 03:29:59"',
                ),
                (
                    ("2018-01-01 03:30:00", "2018-05-16 03:29:59"),
                    'select * from MOCK_DATA where updated_at > "2018-01-01 03:30:00"'
                    + ' and updated_at <= "2018-05-16 03:29:59"',
                ),
            ],
        )
        def test__construct_query_for_partition(self, boundaries, expected_query, extractor):
            allow(extractor)._get_boundaries_for_import.and_return(boundaries)
            actual_query = extractor._construct_query_for_partition(extractor.partition)
            expected_query = " ".join(expected_query.split())
            assert actual_query == expected_query

    @pytest.mark.parametrize("key", ["url", "driver", "user", "password"])
    class TestJDBCOptions(object):
        def test_missing_jdbc_option_raises_error(self, key, default_params):
            del default_params["jdbc_options"][key]
            with pytest.raises(AssertionError) as excinfo:
                JDBCExtractorIncremental(**default_params)
            assert key + " is missing from the jdbc_options." in str(excinfo.value)

        def test_wrong_jdbc_option_raises_error(self, key, default_params):
            default_params["jdbc_options"][key] = 123
            with pytest.raises(AssertionError) as excinfo:
                JDBCExtractorIncremental(**default_params)
            assert key + " has to be provided as a string object." in str(excinfo.value)
