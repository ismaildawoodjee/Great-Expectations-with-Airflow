
# Running this script will populate the great_expectations.yml file
# I ran this inside the container (perhaps could be put as a Task in Airflow?)
# After that this is no longer required

from ruamel import yaml
from pprint import pprint

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

# should be stored as an environment variable
CONNECTION_STRING = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

context = ge.get_context()

datasource_config = {
    "name": "retail_source",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": CONNECTION_STRING,
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "include_schema_name": True,  # to specify schema name when calling BatchRequest
        },
    },
}

checkpoint_config = """
name: retail_source_checkpoint
config_version: 1.0
template_name:
module_name: great_expectations.checkpoint
class_name: Checkpoint
run_name_template: '%Y%m%d-%H%M%S-my-run-name-template'
expectation_suite_name:
batch_request:
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names: []
evaluation_parameters: {}
runtime_configuration: {}
validations:
  - batch_request:
      datasource_name: retail_source
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: ecommerce.retail_profiling
      data_connector_query:
        index: -1
    expectation_suite_name: retail_source_suite
profilers: []
ge_cloud_id:
"""

# pprint(context.get_available_data_asset_names())
# pprint(context.list_expectation_suite_names())

context.test_yaml_config(yaml.dump(datasource_config))
context.add_datasource(**datasource_config)

context.add_checkpoint(**yaml.load(checkpoint_config))
context.run_checkpoint(checkpoint_name="retail_source_checkpoint")

batch_request = RuntimeBatchRequest(
    datasource_name="retail_source",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="ecommerce.retail_profiling",  # this can be anything that identifies this data
    runtime_parameters={
        "query": "SELECT * from ecommerce.retail_profiling LIMIT 1000"
    },
    batch_identifiers={
        "default_identifier_name": "First 1000 rows for profiling retail source data"
    },
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="retail_source_suite"
)

# print(validator.head())