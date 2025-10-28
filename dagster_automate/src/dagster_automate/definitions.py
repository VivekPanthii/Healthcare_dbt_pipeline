from dagster import Definitions
from .defs.resources import dbt_resource
from .defs.assets import extract, load, dbt_analytics, incremental_dbt_models

defs = Definitions(
    assets=[extract, load, dbt_analytics, incremental_dbt_models],
    resources={"dbt": dbt_resource}
)
