from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject

import dagster as dg

dbt_project_directory = "/home/vivekpanthi/DataEngineering/Health_Care_ETL/healthcare_dbt"
dbt_project = DbtProject(project_dir=dbt_project_directory)

dbt_resource = DbtCliResource(project_dir=dbt_project)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "dbt": dbt_resource,
        }
    )


