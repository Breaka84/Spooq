import pytest
import json

from spooq2.pipeline import PipelineFactory, Pipeline
import etl_pipeline_user_params, elt_pipeline_business_params

    
@pytest.mark.parametrize(argnames="pipeline_type", 
                         argvalues=[etl_pipeline_user_params, elt_pipeline_business_params],
                         ids=["ETL Batch Pipeline", "ELT Ad Hoc Pipeline"])
class TestETLBatchPipeline(object):

    @pytest.fixture()
    def pipeline_factory(self, pipeline_type, mocker):
        metadata = pipeline_type.get_metadata()
        pipeline_factory = PipelineFactory()
        mocker.patch.object(pipeline_factory, "get_metadata")
        pipeline_factory.get_metadata.return_value = metadata
        return pipeline_factory
    
    def test_get_pipeline(self, pipeline_factory, pipeline_type):
        context_vars = pipeline_type.get_context_vars()
        assert isinstance(pipeline_factory.get_pipeline(context_vars), Pipeline)
