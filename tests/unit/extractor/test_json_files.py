from builtins import str
from builtins import object
import pytest
from pyspark.sql.dataframe import DataFrame

from spooq.extractor import JSONExtractor
from spooq.extractor.tools import infer_input_path_from_partition


@pytest.fixture()
def default_extractor():
    return JSONExtractor(input_path="some/path")


class TestBasicAttributes(object):
    def test_logger_should_be_accessible(self, default_extractor):
        assert hasattr(default_extractor, "logger")

    def test_name_is_set(self, default_extractor):
        assert default_extractor.name == "JSONExtractor"

    def test_str_representation_is_correct(self, default_extractor):
        assert str(default_extractor) == "Extractor Object of Class JSONExtractor"


class TestPathManipulation(object):
    """Path manipulating Methods"""

    #  fmt: off
    @pytest.mark.parametrize(
        ("input_params", "expected_path"),
        [
            (("base", 20170601), "base/17/06/01/*"),
            (("/base", "20170601"), "/base/17/06/01/*"),
            (("hdfs://nameservice-ha/base/path", "20170601"), "/base/path/17/06/01/*"),
            (("hdfs://nameservice-ha:8020/base/path", "20170601"), "/base/path/17/06/01/*"),
        ],
    )
    #  fmt: on
    def test_infer_input_path_from_partition(self, input_params, expected_path):
        assert expected_path == infer_input_path_from_partition(*input_params)

    #  fmt: off
    @pytest.mark.parametrize(
        ("input_params", "expected_path"),
        [
            (("hdfs://nameservice-ha:8020/full/input/path/provided", None, None), "/full/input/path/provided/*"),
            ((None, "/base/path/to/file", 20180723), "/base/path/to/file/18/07/23/*"),
            ((None, "hdfs://nameservice-ha/base/path/to/file", 20180723), "/base/path/to/file/18/07/23/*"),
        ],
    )
    #  fmt: on
    def test__get_path(self, input_params, expected_path, default_extractor):
        """Chooses whether to use Full Input Path or derive it from Base Path and Partition"""
        assert expected_path == default_extractor._get_path(*input_params)


@pytest.mark.parametrize("input_path", ["data/schema_v1/sequenceFiles", "data/schema_v1/textFiles"])
class TestExtraction(object):
    """Extraction of JSON Files"""

    @pytest.fixture(scope="class")
    def expected_df(self, spark_session):
        df = spark_session.read.parquet("data/schema_v1/parquetFiles/*")
        df = df.drop("birthday")  # duplicated column due to manual date conversions in parquet
        return df

    def test_conversion(self, input_path, expected_df):
        """JSON File is converted to a DataFrame"""
        extractor = JSONExtractor(input_path=input_path)
        assert isinstance(extractor.extract(), DataFrame)

    def test_schema(self, input_path, expected_df):
        """JSON File is converted to the correct schema"""
        extractor = JSONExtractor(input_path=input_path)
        result_df = extractor.extract()
        assert expected_df.schema == result_df.schema

    def test_count(self, input_path, expected_df):
        """Converted DataFrame contains the same Number of Rows as in the Source Data"""
        extractor = JSONExtractor(input_path=input_path)
        assert expected_df.count() == extractor.extract().count()
