from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Union

from .schema import JsonSchema

DbValue = Union[str, int, float, bytes, dict[str, "DbValue"], list["DbValue"], None]


@dataclass
class DbColumn:
    name: str
    schema: JsonSchema
    values: list[DbValue]

    def copy(self):
        return DbColumn(
            name=self.name,
            schema=self.schema,
            values=self.values,
        )


@dataclass
class DbTable:
    name: str
    columns: list[DbColumn]
    expanded_columns: list["ExpandedColumn"]
    join_info: Any = None

    def find_column(self, name: str) -> DbColumn:
        for column in self.columns:
            if column.name == name:
                return column
        raise ValueError(f"Column {name} not found in table {self.name}")

    def find_expanded_column(self, name: str) -> "ExpandedColumn":
        for column in self.expanded_columns:
            if column.name == name:
                return column
        all_column_names = [column.name for column in self.expanded_columns]
        raise ValueError(
            f"Column {name} not found in table {self.name}, available columns: {all_column_names}"
        )

    def copy(self):
        return DbTable(
            name=self.name,
            columns=self.columns.copy(),
            expanded_columns=self.expanded_columns.copy(),
        )

    def __init__(
        self,
        name: str,
        columns: list[DbColumn],
        expanded_columns: list["ExpandedColumn"],
    ):
        self.name = name
        self.columns = columns
        self.expanded_columns = expanded_columns

        assert len(self.columns) > 0
        data_length = len(self.columns[0].values)

        names = set()

        for column in self.columns:
            assert len(column.values) == data_length
            if column.name in names:
                raise ValueError(f"Duplicate column name: {name}")
            names.add(column.name)

    def value_length(self) -> int:
        return len(self.columns[0].values)

    def add_expanded_column(self, column: "ExpandedColumn"):
        assert len(column.values) == self.value_length()

        for expanded_column in self.expanded_columns:
            if expanded_column.name == column.name:
                raise ValueError(
                    f"Column {column.name} already exists in table {self.name}"
                )

        self.expanded_columns.append(column)


@dataclass
class DbDump:
    tables: list[DbTable]

    def find_table(self, name: str) -> DbTable:
        for table in self.tables:
            if table.name == name:
                return table
        raise ValueError(f"Table {name} not found in dump")

    def have_table(self, name: str) -> bool:
        for table in self.tables:
            if table.name == name:
                return True
        return False


class ExpandedJson:
    ty: str
    name_concats: list[str | int | None]


class ExpandOps:
    ArrayIdx = "array_idx"
    ArrayLen = "array_len"
    ArrayFlatten = "array_flatten"
    ArrayExpand = "array_expand"
    ArrayExpandExists = "array_expand_exists"
    ObjectExpand = "object_expand"
    ObjectFieldExists = "object_field_exists"
    DictKey = "dict_key"
    DictValue = "dict_value"
    ArrayDictKey = "array_dict_key"
    ArrayDictValue = "array_dict_value"


@dataclass
class ExpandOp:
    ty: str
    arg: int | str | None


@dataclass
class ExpandedColumn:
    name: str
    original_column: str
    expand_ops: list
    schema: "JsonSchema"
    values: list[DbValue]

    def copy(self):
        return ExpandedColumn(
            name=self.name,
            original_column=self.original_column,
            expand_ops=self.expand_ops.copy(),
            schema=self.schema,
            values=self.values,
        )


def db_merge(left: DbDump, right: DbDump, left_prefix="", right_prefix=""):
    result = DbDump(tables=[])

    for left_table in left.tables:
        left_table_copy = deepcopy(left_table)
        left_table_copy.name = left_prefix + left_table.name
        result.tables.append(left_table_copy)

    for right_table in right.tables:
        right_table_copy = deepcopy(right_table)
        right_table_copy.name = right_prefix + right_table.name
        result.tables.append(right_table_copy)

    return result


def db_merge_logs_and_db(logs: DbDump, db: DbDump):
    return db_merge(logs, db, "log::", "db::")


def shallow_copy_table(table: DbTable) -> DbTable:
    return DbTable(
        name=table.name,
        columns=table.columns.copy(),
        expanded_columns=table.expanded_columns.copy(),
    )


class DbSchema:
    schemas: dict[str, dict[str, JsonSchema]]

    def __init__(self, schemas: dict[str, dict[str, JsonSchema]] | None = None):
        if schemas is None:
            schemas = {}
        self.schemas = schemas

    def to_json(self) -> dict:
        result = {}
        for table_name, table in self.schemas.items():
            result[table_name] = {}
            for column_name, schema in table.items():
                result[table_name][column_name] = schema.to_json()
        return result

    @staticmethod
    def from_json(json: dict) -> "DbSchema":
        schemas = {}
        for table_name, table in json.items():
            schemas[table_name] = {}
            for column_name, schema in table.items():
                schemas[table_name][column_name] = JsonSchema.from_json(schema)
        return DbSchema(schemas)

    def __eq__(self, other) -> bool:
        if not isinstance(other, DbSchema):
            return False

        if len(self.schemas) != len(other.schemas):
            return False

        for table_name, table in self.schemas.items():
            if table_name not in other.schemas:
                return False
            other_table = other.schemas[table_name]
            if len(table) != len(other_table):
                return False
            for column_name, schema in table.items():
                if column_name not in other_table:
                    return False
                if schema != other_table[column_name]:
                    return False

        return True
