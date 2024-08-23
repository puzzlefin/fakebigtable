import pytest
import os

from google.api_core.retry import Retry
from google.cloud import bigtable
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.column_family import MaxVersionsGCRule
from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.row_filters import RowKeyRegexFilter
from google.cloud.bigtable.table import Table

from fakebigtable import FakeBigtableClient


@pytest.fixture(scope="session")
def real_bigtable_instance():
    if not "BIGTABLE_EMULATOR_HOST" in os.environ:
        os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:9035"

    client = bigtable.Client(project="your-project-id", admin=True)
    instance = client.instance("test-instance")

    yield instance


@pytest.fixture(scope="session")
def fake_bigtable_instance():
    client = FakeBigtableClient(project="test-project", admin=True)
    instance = client.instance("test-instance")

    yield instance


@pytest.mark.parametrize("bigtable_instance", ["real", "fake"], indirect=True)
def test_bigtable_operations(bigtable_instance: Instance):
    table: Table = bigtable_instance.table("test-table")
    if table.exists():
        table.delete()

    # Create column family
    cf_id = "cf1"
    table.create(column_families={cf_id: MaxVersionsGCRule(1)})

    # Test inserting and reading rows
    test_key = b"test_key"
    test_value = b"test_value"
    row = table.direct_row(test_key)

    row.set_cell(cf_id, b"col1", test_value)
    row.commit()

    read_row = table.read_row(test_key)
    assert read_row.cell_value(cf_id, b"col1") == test_value

    # Test reading with a filter
    filter_ = RowKeyRegexFilter('^test_.*')
    filtered_rows = list(table.read_rows(filter_=filter_))

    assert len(filtered_rows) == 1
    assert filtered_rows[0].row_key == test_key
    assert filtered_rows[0].cell_value(cf_id, b"col1") == test_value

    # Test deletion
    row.delete()
    row.commit()

    assert table.read_row(test_key) is None


@pytest.fixture
def bigtable_instance(request, real_bigtable_instance, fake_bigtable_instance):
    if request.param == "real":
        return real_bigtable_instance
    elif request.param == "fake":
        return fake_bigtable_instance


@pytest.mark.parametrize("bigtable_instance", ["real", "fake"], indirect=True)
def test_non_existent_column_family_behavior(bigtable_instance: Instance):
    table: Table = bigtable_instance.table("test-table")
    if table.exists():
        table.delete()

    table.create(column_families={"cf1": MaxVersionsGCRule(1)})

    row: DirectRow = table.direct_row(b"test_key")
    row.set_cell("non_existent_cf", b"col1", b"value")

    responses = table.mutate_rows([row], retry=False)
    assert responses[0].code == 13
    assert "unknown family" in responses[0].message

    assert table.read_row(b"test_key") is None


@pytest.mark.parametrize("bigtable_instance", ["real", "fake"], indirect=True)
def test_row_key_collision(bigtable_instance):
    table = bigtable_instance.table("test-table")
    if table.exists():
        table.delete()
    table.create(column_families={"cf1": MaxVersionsGCRule(1)})

    row1 = table.direct_row(b"collision_key")
    row1.set_cell("cf1", b"col1", b"value1")
    row1.commit()

    row2 = table.direct_row(b"collision_key")
    row2.set_cell("cf1", b"col1", b"value2")
    row2.commit()

    assert table.read_row(b"collision_key").cell_value("cf1", b"col1") == b"value2"
