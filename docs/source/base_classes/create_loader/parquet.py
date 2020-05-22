from pyspark.sql import functions as F

from loader import Loader

class ParquetLoader(loader):
    """
    This is a simplified example on how to implement a new loader class.
    Please take your time to write proper docstrings as they are automatically
    parsed via Sphinx to build the HTML and PDF documentation.
    Docstrings use the style of Numpy (via the napoleon plug-in).

    This class uses the :meth:`pyspark.sql.DataFrameWriter.parquet` method internally.

    Examples
    --------
    input_df = some_extractor_instance.extract()
    output_df = some_transformer_instance.transform(input_df)
    ParquetLoader(
        path="data/parquet_files",
        partition_by="dt",
        explicit_partition_values=20200201,
        compression=""gzip""
    ).load(output_df)

    Parameters
    ----------
    path: :any:`str`
        The path to where the loader persists the output parquet files.
        If partitioning is set, this will be the base path where the partitions 
        are stored.

    partition_by: :any:`str` or :any:`list` of (:any:`str`)
        The column name or names by which the output should be partitioned.
        If the partition_by parameter is set to None, no partitioning will be 
        performed.
        Defaults to "dt"

    explicit_partition_values: :any:`str` or :any:`int` 
                                or :any:`list` of (:any:`str` and :any:`int`)
        Only allowed if partition_by is not None.
        If explicit_partition_values is not None, the dataframe will
            * overwrite the partition_by columns values if it already exists or
            * create and fill the partition_by columns if they do not yet exist
        Defaults to None

    compression: :any:`str`
        The compression codec used for the parquet output files.
        Defaults to "snappy"

    
    Raises
    ------
    :any:`exceptions.AssertionError`:
        explicit_partition_values can only be used when partition_by is not None
    :any:`exceptions.AssertionError`:
        explicit_partition_values and partition_by must have the same length
    """

    def __init__(self, path, partition_by="dt", explicit_partition_values=None, compression_codec="snappy"):
        super(ParquetLoader, self).__init__()
        self.path = path
        self.partition_by = partition_by
        self.explicit_partition_values = explicit_partition_values
        self.compression_codec = compression_codec
        if explicit_partition_values is not None:
            assert (partition_by is not None,
                "explicit_partition_values can only be used when partition_by is not None")
            assert (len(partition_by) == len(explicit_partition_values),
                "explicit_partition_values and partition_by must have the same length")

    def load(self, input_df):
        self.logger.info("Persisting DataFrame as Parquet Files to " + self.path)

        if isinstance(self.explicit_partition_values, list):
            for (k, v) in zip(self.partition_by, self.explicit_partition_values):
                input_df = input_df.withColumn(k, F.lit(v))
        elif isinstance(self.explicit_partition_values, basestring):
            input_df = input_df.withColumn(self.partition_by, F.lit(self.explicit_partition_values))

        input_df.write.parquet(
            path=self.path,
            partitionBy=self.partition_by,
            compression=self.compression_codec
        )
