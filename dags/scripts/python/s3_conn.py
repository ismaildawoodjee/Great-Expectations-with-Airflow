import great_expectations as ge
from great_expectations.core.batch import BatchRequest

from pprint import pprint
from great_expectations.cli.datasource import (
    sanitize_yaml_and_save_datasource,
)

context = ge.get_context()

datasource_yaml = """
name: retail_load
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetS3DataConnector
    bucket: greatex-bucket
    prefix: raw/retail/
    default_regex:
      group_names: 
        - prefix
        - year
        - month
        - day
        - data_asset_name
      pattern: (.*)/(\d{4})/(\d{2})/(\d{2})/(.*)\.csv
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
"""

batch_request = BatchRequest(
    datasource_name="retail_load",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="retail_profiling",
    batch_spec_passthrough={
        "reader_method": "read_csv",
        "reader_options": {
            "nrows": 1000
        }
    }
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="retail_source_suite"
)

# print(validator.head())

context.test_yaml_config(yaml_config=datasource_yaml)
sanitize_yaml_and_save_datasource(context, datasource_yaml, overwrite_existing=True)