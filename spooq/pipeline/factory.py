"""
To decrease the complexity of building data pipelines for data engineers, an expert system or
business rules engine can be used to automatically build and configure a data pipeline based on
context variables, groomed metadata, and relevant rules.
"""
from __future__ import print_function

from builtins import object
import requests
import json
from spooq.pipeline import Pipeline
import spooq.extractor as E
import spooq.transformer as T
import spooq.loader as L


class PipelineFactory(object):
    """
    Provides an interface to automatically construct pipelines for Spooq.

    Example
    -------
    >>> pipeline_factory = PipelineFactory()
    >>>
    >>> #  Fetch user data set with applied mapping, filtering,
    >>> #  and cleaning transformers
    >>> df = pipeline_factory.execute({
    >>>      "entity_type": "user",
    >>>      "date": "2018-10-20",
    >>>      "time_range": "last_day"})
    >>>
    >>> #  Load user data partition with applied mapping, filtering,
    >>> #  and cleaning transformers to a hive database
    >>> pipeline_factory.execute({
    >>>      "entity_type": "user",
    >>>      "date": "2018-10-20",
    >>>      "batch_size": "daily"})

    Attributes
    ----------
    url : :any:`str`, (Defaults to "http://localhost:5000/pipeline/get")
        The end point of an expert system which will be called to infer names and parameters.

    Note
    ----
    PipelineFactory is only responsible for querying an expert system with provided parameters
    and constructing a Spooq pipeline out of the response. It does not have any reasoning capabilities
    itself! It requires therefore a HTTP service responding with a JSON object containing following structure:

    ::

        {
            "extractor": {"name": "Type1Extractor", "params": {"key 1": "val 1", "key N": "val N"}},
            "transformers": [
                {"name": "Type1Transformer", "params": {"key 1": "val 1", "key N": "val N"}},
                {"name": "Type2Transformer", "params": {"key 1": "val 1", "key N": "val N"}},
                {"name": "Type3Transformer", "params": {"key 1": "val 1", "key N": "val N"}},
                {"name": "Type4Transformer", "params": {"key 1": "val 1", "key N": "val N"}},
                {"name": "Type5Transformer", "params": {"key 1": "val 1", "key N": "val N"}},
            ],
            "loader": {"name": "Type1Loader", "params": {"key 1": "val 1", "key N": "val N"}}
        }

    Hint
    ----
    There is an experimental implementation of an expert system which complies with the requirements
    of PipelineFactory called `spooq_rules`. If you are interested, please ask the author of Spooq about it.
    """

    def __init__(self, url="http://localhost:5000/pipeline/get"):
        self.url = url

    def execute(self, context_variables):
        """
        Fetches a ready-to-go pipeline instance via :py:meth:`get_pipeline`
        and executes it.

        Parameters
        ----------
            context_variables : :py:class:`dict`
                These collection of parameters should describe the current context about the use case
                of the pipeline. Please see the examples of the PipelineFactory class'
                documentation.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            If the loader component is by-passed (in the case of ad_hoc use cases).

        :any:`None`
            If the loader component does not return a value (in the case of persisting data).
        """
        pipeline = self.get_pipeline(context_variables)
        return pipeline.execute()

    def get_metadata(self, context_variables):
        """
        Sends a POST request to the defined endpoint (`url`) containing the
        supplied context variables.

        Parameters
        ----------
        context_variables : :py:class:`dict`
            These collection of parameters should describe the current context about the use case
            of the pipeline. Please see the examples of the PipelineFactory class'
            documentation.

        Returns
        -------
        :py:class:`dict`
            Names and parameters of each ETL component to construct a Spooq pipeline
        """
        return requests.post(self.url, json=context_variables).json()

    def get_pipeline(self, context_variables):
        """
        Fetches the necessary metadata via :py:meth:`get_metadata` and
        returns a ready-to-go pipeline instance.

        Parameters
        ----------
        context_variables : :py:class:`dict`
            These collection of parameters should describe the current context about the use case
            of the pipeline. Please see the examples of the PipelineFactory class'
            documentation.

        Returns
        -------
        :py:class:`~spooq.Pipeline`
            A Spooq pipeline instance which is fully configured and can still be
            adapted and consequently executed.
        """
        metadata = self.get_metadata(context_variables)
        pipeline = Pipeline()
        extractor = self._get_extractor(metadata)
        pipeline.set_extractor(extractor)
        transformers = self._get_transformers(metadata)
        pipeline.add_transformers(transformers)
        loader = self._get_loader(metadata)
        if not loader:
            pipeline.bypass_loader = True
        else:
            pipeline.set_loader(loader)
        return pipeline

    def _get_extractor(self, magic_data):
        extractor_class = getattr(E, magic_data.get("extractor", {}).get("name", ""))
        extractor_params = magic_data.get("extractor", {}).get("params", "")
        return extractor_class(**extractor_params)

    def _get_transformers(self, magic_data):
        transformers = []
        for transformer in magic_data["transformers"]:
            transformer_class = getattr(T, transformer["name"])
            print(transformer_class)
            transformers.append(transformer_class(**transformer["params"]))
        return transformers

    def _get_loader(self, magic_data):
        loader_name = magic_data.get("loader", {}).get("name", "")
        if loader_name == "ByPass":
            return False
        loader_class = getattr(L, loader_name)
        loader_params = magic_data.get("loader", {}).get("params", "")
        return loader_class(**loader_params)
