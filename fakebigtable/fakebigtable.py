import time
from typing import Dict, Generator, Optional
import re
from google.cloud.bigtable.column_family import MaxVersionsGCRule
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.row_set import RowSet


class FakeStatus:
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message


class FakeColumnFamily:
    def __init__(self, family_name: str, gc_rule):
        self.family_name = family_name
        self.gc_rule = gc_rule


class FakeCell:
    def __init__(self, value: bytes, timestamp: float = None):
        self.value = value
        self.timestamp = timestamp or time.time()


class FakeRow:
    def __init__(self, key: bytes, table: "FakeBigtableTable"):
        self.row_key = key
        self.table = table
        self.cells = {}
        self.pending_cells = {}

    def set_cell(
        self,
        column_family_id: bytes,
        column: bytes,
        value: bytes,
        timestamp: float = None,
    ) -> None:
        self.pending_cells.setdefault((column_family_id, column), []).append(
            FakeCell(value, timestamp)
        )

    def cell_value(self, column_family_id: bytes, column: bytes) -> Optional[bytes]:
        cells = self.cells.get((column_family_id, column), [])
        return cells[-1].value if cells else None

    def apply_cell_limit(self, num_cells: int | None) -> None:
        if num_cells is not None:
            for key, cell_list in self.cells.items():
                self.cells[key] = cell_list[:num_cells]

    def _has_cells(self) -> bool:
        for cells in self.cells.values():
            if cells:
                return True
        return False

    def delete(self) -> None:
        self.pending_cells.clear()
        self.cells.clear()

    def commit(self) -> None:
        for key, new_cells in list(self.pending_cells.items()):
            try:
                gc_rule = self.table.column_families[key[0]].gc_rule
            except KeyError:
                self.pending_cells.clear()
                return FakeStatus(13, "unknown family")

            existing_cells = self.cells.setdefault(key, [])
            existing_cells.extend(new_cells)

            if isinstance(gc_rule, MaxVersionsGCRule):
                del existing_cells[: -gc_rule.max_num_versions]

        self.pending_cells.clear()

        # If there are no cells left after commit, remove the row from the table
        if not self.cells:
            self.table.rows.pop(self.row_key, None)
        return FakeStatus(0, "OK")

    def _copy(self):
        row = FakeRow(self.row_key, self.table)
        row.cells = self.cells.copy()
        return row


class FakeBigtableTable:
    def __init__(self):
        self._exists = True
        self.rows: Dict[bytes, FakeRow] = {}
        self.column_families: Dict[bytes, FakeColumnFamily] = {}

    def create_column_family(self, family_name: bytes, gc_rule) -> None:
        if family_name in self.column_families:
            raise ValueError(f"Column family '{family_name}' already exists.")
        self.column_families[family_name] = FakeColumnFamily(family_name, gc_rule)

    def delete_column_family(self, family_name: bytes) -> None:
        if family_name not in self.column_families:
            raise ValueError(f"Column family '{family_name}' does not exist.")
        del self.column_families[family_name]

    def list_column_families(self) -> Dict[bytes, FakeColumnFamily]:
        return self.column_families

    def direct_row(self, key: bytes) -> FakeRow:
        if key not in self.rows:
            self.rows[key] = FakeRow(key, self)
        return self.rows[key]

    def read_row(self, key: bytes, filter_=None) -> Optional[FakeRow]:
        row_set = RowSet()
        row_set.add_row_key(key)
        result_iter = iter(self.read_rows(filter_=filter_, row_set=row_set))
        row = next(result_iter, None)
        if next(result_iter, None) is not None:
            raise ValueError("More than one row was returned.")
        return row

    def read_rows(
        self,
        start_key=None,
        end_key=None,
        limit=None,
        filter_=None,
        end_inclusive=False,
        row_set=None,
        retry=None,
    ) -> Generator[FakeRow, None, None]:
        rex = None
        if hasattr(filter_, "regex"):
            rex = re.compile(filter_.regex)

        num_cells = None
        if isinstance(filter_, CellsColumnLimitFilter):
            num_cells = filter_.num_cells

        for key in sorted(self.rows):
            if start_key is not None and key < start_key:
                continue
            if end_key is not None and key > end_key:
                continue
            if end_key is not None and not end_inclusive and key == end_key:
                continue
            if row_set is not None and key not in row_set.row_keys:
                continue
            if limit is not None and limit <= 0:
                break
            if rex is None or rex.match(key):
                row = self.rows[key]._copy()
                row.apply_cell_limit(num_cells)
                if not row._has_cells():
                    continue
                yield row
                if limit is not None:
                    limit -= 1

    @staticmethod
    def mutate_rows(rows: list[FakeRow], retry: bool = True) -> None:
        res = []
        for row in rows:
            res.append(row.commit())
        return res

    def truncate(self) -> None:
        self.rows.clear()

    def exists(self) -> bool:
        return self._exists

    def create(
        self, column_families: Optional[Dict[str, "MaxVersionsGCRule"]] = {}
    ) -> None:
        """Simulates creating a Bigtable table with specified column families."""
        if column_families:
            for family_name, gc_rule in column_families.items():
                self.create_column_family(family_name, gc_rule)
        self._exists = True

    def delete(self) -> None:
        self.rows.clear()
        self.column_families.clear()
        self._exists = False

    def drop_by_prefix(self, row_key_prefix: bytes) -> None:
        keys_to_delete = [key for key in self.rows if key.startswith(row_key_prefix)]
        for key in keys_to_delete:
            del self.rows[key]


class FakeBigtableInstance:
    def __init__(self, instance_id: str):
        self.instance_id = instance_id
        self.tables = {}

    def table(self, table_id: str) -> FakeBigtableTable:
        if table_id not in self.tables:
            self.tables[table_id] = FakeBigtableTable()
        return self.tables[table_id]

    def list_tables(self) -> list:
        return list(self.tables.values())

    def create(self) -> None:
        # No-op in fake; assume the instance exists when instantiated
        pass

    def delete(self) -> None:
        self.tables.clear()


class FakeBigtableClient:
    def __init__(self, project: str, admin: bool = False):
        self.project = project
        self.admin = admin
        self.instances = {}

    def instance(self, instance_id: str) -> "FakeBigtableInstance":
        if instance_id not in self.instances:
            self.instances[instance_id] = FakeBigtableInstance(instance_id)
        return self.instances[instance_id]
