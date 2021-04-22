from __future__ import absolute_import
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

from .extractor import Extractor
from .tools import remove_hdfs_prefix, fix_suffix, infer_input_path_from_partition


class JSONExtractor(Extractor):
    """
    The JSONExtractor class provides an API to extract data stored as JSON format,
    deserializes it into a PySpark dataframe and returns it. Currently only
    single-line JSON files are supported, stored either as textFile or sequenceFile.

    Examples
    --------
    >>> from spooq import extractor as E

    >>> extractor = E.JSONExtractor(input_path="tests/data/schema_v1/sequenceFiles")
    >>> extractor.input_path == "tests/data/schema_v1/sequenceFiles" + "/*"
    True

    >>> extractor = E.JSONExtractor(
    >>>     base_path="tests/data/schema_v1/sequenceFiles",
    >>>     partition="20200201"
    >>> )
    >>> extractor.input_path == "tests/data/schema_v1/sequenceFiles" + "/20/02/01" + "/*"
    True

    Parameters
    ----------
    input_path : :any:`str`
        The path from which the JSON files should be loaded ("/\\*" will be added if omitted)
    base_path : :any:`str`
        Spooq tries to infer the ``input_path`` from the ``base_path`` and the ``partition`` if the
        ``input_path`` is missing.
    partition : :any:`str` or :any:`int`
        Spooq tries to infer the ``input_path`` from the ``base_path`` and the ``partition`` if the
        ``input_path`` is missing.
        Only daily partitions in the form of "YYYYMMDD" are supported. e.g., "20200201" => <base_path> + "/20/02/01/\\*"

    Returns
    -------
    :any:`pyspark.sql.DataFrame`
        The extracted data set as a PySpark DataFrame

    Raises
    ------
    :any:`AttributeError`
        Please define either ``input_path`` or ``base_path`` and ``partition``

    Warning
    ---------
    Currently only single-line JSON files stored as SequenceFiles or TextFiles are supported!

    Note
    ----
    The init method checks which input parameters are provided and derives the final input_path
    from them accordingly.

    If ``input_path`` is not :any:`None`:
        Cleans ``input_path`` and returns it as the final ``input_path``

    Elif ``base_path`` and ``partition`` are not :any:`None`:
        Cleans ``base_path``, infers the sub path from the ``partition``
        and returns the combined string as the final ``input_path``

    Else:
        Raises an :any:`AttributeError`

    """

    def __init__(self, input_path=None, base_path=None, partition=None):
        super(JSONExtractor, self).__init__()
        self.input_path = self._get_path(input_path=input_path, base_path=base_path, partition=partition)
        self.base_path = base_path
        self.partition = partition
        self.spark = (
            SparkSession.Builder()
            .enableHiveSupport()
            .appName("spooq.extractor: {nm}".format(nm=self.name))
            .getOrCreate()
        )

    def extract(self):
        """
        This is the Public API Method to be called for all classes of Extractors

        Parameters
        ----------

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            Complex PySpark DataFrame deserialized from the input JSON Files

        """
        self.logger.info("Loading Raw RDD from: " + self.input_path)
        rdd_raw = self._get_raw_rdd(self.input_path)
        rdd_strings = self._get_values_as_string_rdd(rdd_raw)
        return self._convert_rdd_to_df(rdd_strings)

    def _get_path(self, input_path=None, base_path=None, partition=None):
        """
        Checks which input parameters are provided and derives the final input_path from them.

        If :py:data:`input_path` is not :any:`None`:
            Cleans :py:data:`input_path` and returns it as the final input_path

        If :py:data:`base_path` and :py:data:`partition` are not :any:`None`:
            Cleans :py:data:`base_path`, infers the sub path from the :py:data:`partition` and returns
            the combined String as the final input_path

        If none of the above holds true, an Exception is raised

        Parameters
        ----------
        input_path : :any:`str`
        base_path : :any:`str`
        partition : :any:`str` or :any:`int`

        Returns
        -------
        :any:`str`
            The final input_path to be used for Extraction.

        Raises
        ------
        :py:class:`AttributeError`
            Please define either (input_path) or (base_path and partition)

        Examples
        --------
        >>> _get_path(input_path=u'/user/furia_salamandra_faerfax/data')
        u'/user/furia_salamandra_faerfax/data/*'

        >>> _get_path(base_path=u'/user/furia_salamandra_faerfax/data', partition=20180101)
        u'/user/furia_salamandra_faerfax/data/18/01/01/*'

        See Also
        --------
        :py:meth:`fix_suffix`
        :py:meth:`remove_hdfs_prefix`

        """
        if input_path:
            return fix_suffix(remove_hdfs_prefix(input_path))
        elif base_path and partition:
            return infer_input_path_from_partition(base_path=base_path, partition=partition)
        else:
            error_msg = "Please define either (input_path) or (base_path and partition)"
            self.logger.error(error_msg)
            raise AttributeError(error_msg)

    def _get_raw_rdd(self, input_path):
        """
        Loads TextFiles containing JSON Strings from :py:data:`input_path` and parallelizes them
        into an :any:pyspark.RDD`.

        Parameters
        ----------
        input_path : :any:`str`

        Returns
        -------
        :any:`pyspark.RDD`
            Output RDD with one JSON String per Record

        See Also
        --------
        pyspark.SparkContext.textFile

        """
        try:
            return self._get_raw_sequence_rdd(input_path)
        except Py4JJavaError:
            return self._get_raw_text_rdd(input_path)

    def _get_raw_text_rdd(self, input_path):
        self.logger.debug("Fetching TextFile containing JSON")
        return self.spark.sparkContext.textFile(input_path)

    def _get_raw_sequence_rdd(self, input_path):
        self.logger.debug("Fetching SequenceFile containing JSON")
        return self.spark.sparkContext.sequenceFile(input_path).map(lambda k_v: k_v[1].decode("utf-8"))

    def _convert_rdd_to_df(self, rdd_strings):
        """
        Converts the input RDD :py:data:`rdd_strings` to a DataFrame with inferred Structure
        and DataTypes and returns it.

        Parameters
        ----------
        rdd_strings : :any:`pyspark.RDD`
            Input RDD containing only unicode JSON Strings per Record

        Returns
        -------
        :any:`pyspark.sql.DataFrame`
            Complex DataFrame with set of all found Attributes from input JSON Files

        See Also
        --------
        pyspark.sql.DataFrameReader.json

        """
        self.logger.debug("Deserializing JSON from String RDD to DataFrame")
        return self.spark.read.json(rdd_strings)

    def _get_values_as_string_rdd(self, rdd_raw):
        """
        Removes newline ``\n`` and carriage return ``\r`` characters.

        Parameters
        ----------
        rdd_raw : :any:`pyspark.RDD`

        Returns
        -------
        :any:`pyspark.RDD`
            Output RDD with one JSON String per Record

        """
        self.logger.debug("Cleaning JSON String RDD (selecting values, " + "removing newline and carriage return)")
        return rdd_raw.map(lambda v: v.replace("\\n", " ").replace("\\r", ""))
