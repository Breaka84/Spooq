"""
This type of object glues the aforementioned processes together and  extracts, transforms
(Transformer chain possible) and loads the data from start to end.
"""

from builtins import str
from builtins import object
import logging


class Pipeline(object):
    """
    Represents a Pipeline of an Extractor, (multiple) Transformers and a Loader Object.

    Attributes
    ----------
        extractor : Subclass of :py:class:`spooq.extractor.Extractor`
            The entry point of the Pipeline. Extracts a DataFrame from a Source.
        transformers : List of Subclasses of :py:class:`spooq.transformer.Transformer` Objects
            The Data Wrangling Part of the Pipeline. A chain of Transformers, a single Transformer
            or a PassThrough Transformer can be set and used.
        loader : Subclass of :py:class:`spooq.loader.Loader`
            The exit point of the Pipeline. Loads a DataFrame to a target Sink.
        name : :any:`str`
            Sets the `__name__` of the class' type as `name`, which is essentially the Class' Name.
        logger : :any:`logging.Logger`
            Shared, class level logger for all instances.

    Example
    -------
    >>> from spooq.pipeline import Pipeline
    >>> import spooq.extractor as   E
    >>> import spooq.transformer as T
    >>> import spooq.loader as      L
    >>>
    >>> #  Definition how the output table should look like and where the attributes come from:
    >>> users_mapping = [
    >>>     ("id",              "id",                     "IntegerType"),
    >>>     ("guid",            "guid",                   "StringType"),
    >>>     ("forename",        "attributes.first_name",  "StringType"),
    >>>     ("surename",        "attributes.last_name",   "StringType"),
    >>>     ("gender",          "attributes.gender",      "StringType"),
    >>>     ("has_email",       "attributes.email",       "StringBoolean"),
    >>>     ("has_university",  "attributes.university",  "StringBoolean"),
    >>>     ("created_at",      "meta.created_at_ms",     "timestamp_ms_to_s"),
    >>> ]
    >>>
    >>> #  The main object where all steps are defined:
    >>> users_pipeline = Pipeline()
    >>>
    >>> #  Defining the EXTRACTION:
    >>> users_pipeline.set_extractor(E.JSONExtractor(
    >>>     input_path="tests/data/schema_v1/sequenceFiles"
    >>> ))
    >>>
    >>> #  Defining the TRANSFORMATION:
    >>> users_pipeline.add_transformers([
    >>>     T.Mapper(mapping=users_mapping),
    >>>     T.ThresholdCleaner(thresholds={"created_at": {
    >>>                                                 "min": 0,
    >>>                                                 "max": 1580737513,
    >>>                                                 "default": None}}),
    >>>     T.NewestByGroup(group_by="id", order_by="created_at")
    >>> ])
    >>>
    >>> #  Defining the LOAD:
    >>> users_pipeline.set_loader(L.HiveLoader(
    >>>     db_name="users_and_friends",
    >>>     table_name="users",
    >>>     partition_definitions=[{
    >>>         "column_name": "dt",
    >>>         "column_type": "IntegerType",
    >>>         "default_value": 20200201}],
    >>>     repartition_size=10,
    >>> ))
    >>>
    >>> #  Executing the whole ETL pipeline
    >>> users_pipeline.execute()
    """

    def __init__(self, input_df=None, bypass_loader=False):
        if input_df is not None:
            self.bypass_extractor = True
            self.input_df = input_df
        else:
            self.bypass_extractor = False

        self.bypass_loader = bypass_loader
        self.extractor = None
        self.transformers = []
        self.loader = None

        self.name = type(self).__name__
        self.logger = logging.getLogger("spooq")
        self.logger.info("New {cls_name} Instance created\n".format(cls_name=str(self.name)) + str(self))

    def execute(self):
        """
        Executes the whole Pipeline at once.

        Extracts from the Source, transformes the DataFrame and loads it into a target Sink.

        Returns
        -------
        input_df : :any:`pyspark.sql.DataFrame`
            **If** the ``bypass_loader`` attribute was set to True in the Pipeline class,
            the output DataFrame from the Transformer(s) will be directly returned.

        Note
        ----
        This method does not take ANY input parameters. All needed parameters are defined
        at the initialization phase.
        """
        extracted_df = self.extract()
        transformed_df = self.transform(extracted_df)
        return self.load(transformed_df)

    def extract(self):
        """Calls the extract Method on the Extractor Object.

        Returns
        -------
        :any:`pyspark.sql.DataFrame`
            The output_df from the Extractor used as the input for the Transformer (chain).
        """
        if self.bypass_extractor:
            return self.input_df
        else:
            return self.extractor.extract()

    def transform(self, input_df):
        """
        Calls the transform Method on the Transformer Object(s) in the order of importing the
        Objects while passing the DataFrame.

        Parameters
        ----------
        input_df : :any:`pyspark.sql.DataFrame`
            The output DataFrame of the Extractor Object.

        Returns
        -------
        :any:`pyspark.sql.DataFrame`
            The input DataFrame for the Loader.
        """
        transformed_df = input_df

        for transformer in self.transformers:
            transformed_df = transformer.transform(transformed_df)

        return transformed_df

    def load(self, input_df):
        """
        Calls the load Method on the Loader Object.

        Parameters
        ----------
        input_df : :any:`pyspark.sql.DataFrame`
            The output DataFrame from the Transformer(s).

        Returns
        -------
        input_df : :any:`pyspark.sql.DataFrame`
            **If** the ``bypass_loader`` attribute was set to True in the Pipeline class,
            the output DataFrame from the Transformer(s) will be directly returned.
        """
        if self.bypass_loader:
            return input_df
        else:
            return self.loader.load(input_df)

    def set_extractor(self, extractor):
        """
        Sets an Extractor Object to be used within the Pipeline.

        Parameters
        ----------
        extractor : Subclass of :py:class:`spooq.extractor.Extractor`
            An already initialized Object of any Subclass of spooq.extractor.Extractor.

        Raises
        ------
        :any:`exceptions.AssertionError`:
            An input_df was already provided which bypasses the extraction action
        """
        assert not self.bypass_extractor, "An input_df was already provided which bypasses the extraction action"
        self.extractor = extractor

    def add_transformers(self, transformers):
        """
        Adds a list of Transformer Objects to be used within the Pipeline.

        Parameters
        ----------
        transformer : :any:`list` of Subclass of :py:class:`spooq.transformer.Transformer`
            Already initialized Object of any Subclass of spooq.transformer.Transformer.
        """

        try:
            self.transformers.extend(transformers)
        except TypeError:
            self.transformers.append(transformers)

    def clear_transformers(self):
        """
        Clears the list of already added Transformers.
        """
        self.transformers = []

    def set_loader(self, loader):
        """
        Sets an Loader Object to be used within the Pipeline.

        Parameters
        ----------
        loader : Subclass of :py:class:`spooq.loader.Loader`
            An already initialized Object of any Subclass of spooq.loader.Loader.

        Raises
        ------
        :any:`exceptions.AssertionError`:
            You can not set a loader if the `bypass_loader` parameter is set.
        """
        assert not self.bypass_loader, "You can not set a loader if the `bypass_loader` parameter is set."
        self.loader = loader

    def __str__(self):
        return """spooq.pipeline Object
                Used Extractor:    {extr}
                Used Transformers: {trans}
                Used Loader:       {ldr}
                """.format(
            extr=str(self.extractor),
            trans=str(["{e}\n".format(e=extr) for extr in self.transformers]),
            ldr=str(self.loader),
        )
