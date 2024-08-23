import time
from typing import Dict, Generator, Optional
import re
from google.cloud.bigtable.column_family import MaxVersionsGCRule


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
    def __init__(self, key: bytes, table: 'FakeBigtableTable'):
        self.row_key = key
        self.table = table
        self.cells = {}
        self.pending_cells = {}

    def set_cell(self, column_family_id: bytes, column: bytes, value: bytes, timestamp: float = None) -> None:
        self.pending_cells.setdefault((column_family_id, column), []).append(FakeCell(value, timestamp))

    def cell_value(self, column_family_id: bytes, column: bytes) -> Optional[bytes]:
        cells = self.cells.get((column_family_id, column), [])
        return cells[-1].value if cells else None

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
                del existing_cells[:-gc_rule.max_num_versions]

        self.pending_cells.clear()

        # If there are no cells left after commit, remove the row from the table
        if not self.cells:
            self.table.rows.pop(self.row_key, None)
        return FakeStatus(0, "OK")


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

    def read_row(self, key: bytes) -> Optional[FakeRow]:
        row = self.rows.get(key)
        if row and row.cells:
            return row

    def read_rows(self, filter_=None) -> Generator[FakeRow, None, None]:
        if filter_:
            rex = re.compile(filter_.regex.decode())
            for key in sorted(self.rows):
                if rex.match(key.decode()):
                    yield self.rows[key]
        else:
            for row in self.rows.values():
                yield row

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

    def create(self, column_families: Optional[Dict[str, 'MaxVersionsGCRule']] = {}) -> None:
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

    def instance(self, instance_id: str) -> 'FakeBigtableInstance':
        if instance_id not in self.instances:
            self.instances[instance_id] = FakeBigtableInstance(instance_id)
        return self.instances[instance_id]
