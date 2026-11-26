from pathlib import Path

from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parents[4] / "transform" / "evd_transform"
DBT_PROFILES_DIR = Path(__file__).parents[4] / "transform"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"


@dbt_assets(manifest=DBT_MANIFEST)
def evd_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
