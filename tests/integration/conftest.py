import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark():
    return (
        DatabricksSession.builder
        .serverless()
        .profile("dbc-c04a5ad1-7844")
        .getOrCreate()
    )