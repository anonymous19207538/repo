from dataclasses import dataclass

from .db import DbTable, DbValue, ExpandedColumn, ExpandOp, ExpandOps
from .expand_mapper import expand_map_with_none, expand_name, expand_schema
from .schema import JsonSchemaTypes
from .induction import JsonSchemaInducer

def derive_field_values(
    expanded_column: ExpandedColumn, expand_op: ExpandOp
) -> list[DbValue]:
    return [expand_map_with_none(value, expand_op) for value in expanded_column.values]


def derive_column(
    original: ExpandedColumn,
    ty: str,
    arg: int | str | None,
):
    expand_op = ExpandOp(ty=ty, arg=arg)
    name_postfix = expand_name(expand_op)
    new_schema = expand_schema(original.schema, expand_op)
    new_values = derive_field_values(original, expand_op)

    return ExpandedColumn(
        name=original.name + name_postfix,
        original_column=original.original_column,
        expand_ops=original.expand_ops + [expand_op],
        schema=new_schema,
        values=new_values,
    )


class DbExpander:
    def __init__(
        self,
        object_expand_exists: bool = False,
        array_expand_max: int = 0,
        array_of_object_expand_exists: bool = False,
        array_expand_length: bool = False,
    ):
        self.object_expand_exists = object_expand_exists
        self.array_expand_max = array_expand_max
        self.array_of_object_expand_exists = array_of_object_expand_exists
        self.array_expand_length = array_expand_length
        self.inducer = JsonSchemaInducer()

    def compress_uniform_array_expanded_column(self, table: DbTable):
        for column in table.expanded_columns:
            if column.schema.ty == JsonSchemaTypes.Array and column.schema.is_basic():
                compressed_column_values = self.compress_uniform_array(column.values)
                if compressed_column_values != []:
                    schema = self.inducer.induce_json_schema(compressed_column_values)
                    expanded_column = ExpandedColumn(
                        name=column.name.replace('[]',''),
                        original_column=column.name,
                        expand_ops=[],
                        schema=schema,
                        values=compressed_column_values,
                    )
                    table.add_expanded_column(expanded_column)

                
    def compress_uniform_array(self, array_column_values: list[DbValue]) -> list[DbValue]:
        values = []
        for array in array_column_values:
            if array == None:
                values.append(None)
            else:
                set_values = set(array)
                if len(set_values) == 1:
                    values.append(list(set_values)[0])
                elif len(set_values) == 0:
                    values.append(None)
                else:
                    return []
        return values
            
                

    def expand_table(self, table: DbTable):
        for column in table.columns:
            expanded_column = ExpandedColumn(
                name=column.name,
                original_column=column.name,
                expand_ops=[],
                schema=column.schema,
                values=column.values,
            )
            self.expand_add_column(table, expanded_column)

    def expand_add_column(self, table: DbTable, column: ExpandedColumn):
        table.add_expanded_column(column)
        self.expand_one_column(table, column)

    def expand_one_column(self, table: DbTable, expanded_column: ExpandedColumn):
        schema = expanded_column.schema

        if schema.ty == JsonSchemaTypes.Object:
            self.expand_one_column_object(table, expanded_column)
        elif schema.ty == JsonSchemaTypes.Array:
            self.expand_one_column_array(table, expanded_column)
        elif schema.ty == JsonSchemaTypes.Dict:
            self.expand_one_column_dict(table, expanded_column)

    def expand_one_column_object(self, table: DbTable, expanded_column: ExpandedColumn):
        schema = expanded_column.schema
        assert schema.ty == JsonSchemaTypes.Object
        assert schema.fields is not None

        for field in schema.fields:
            column = derive_column(expanded_column, ExpandOps.ObjectExpand, field.name)
            self.expand_add_column(table, column)
            if self.object_expand_exists and not field.always_exists:
                column = derive_column(
                    expanded_column, ExpandOps.ObjectFieldExists, field.name
                )
                self.expand_add_column(table, column)

    def expand_one_column_array(self, table: DbTable, expanded_column: ExpandedColumn):
        schema = expanded_column.schema
        assert schema.ty == JsonSchemaTypes.Array
        assert schema.array_element_schema is not None

        if self.array_expand_length:
            self.expand_one_column_array_length(table, expanded_column)
        self.expand_one_column_array_contents(table, expanded_column)
        if schema.array_element_schema.ty == JsonSchemaTypes.Array:
            self.expand_one_column_array_of_array(table, expanded_column)
        elif schema.array_element_schema.ty == JsonSchemaTypes.Object:
            self.expand_one_column_array_of_object(table, expanded_column)
        elif schema.array_element_schema.ty == JsonSchemaTypes.Dict:
            self.expand_one_column_array_of_dict(table, expanded_column)

    def expand_one_column_array_length(
        self, table: DbTable, expanded_column: ExpandedColumn
    ):
        column = derive_column(expanded_column, ExpandOps.ArrayLen, None)
        self.expand_add_column(table, column)

    def expand_one_column_array_contents(
        self, table: DbTable, expanded_column: ExpandedColumn
    ):
        schema = expanded_column.schema

        assert schema.len_max is not None
        assert schema.ty == JsonSchemaTypes.Array

        for idx in range(min(self.array_expand_max, schema.len_max)):
            column = derive_column(expanded_column, ExpandOps.ArrayIdx, idx)
            self.expand_add_column(table, column)

    def expand_one_column_array_of_array(
        self, table: DbTable, expanded_column: ExpandedColumn
    ):
        schema = expanded_column.schema

        assert schema.ty == JsonSchemaTypes.Array
        assert schema.array_element_schema is not None
        assert schema.array_element_schema.ty == JsonSchemaTypes.Array

        column = derive_column(expanded_column, ExpandOps.ArrayFlatten, None)
        self.expand_add_column(table, column)

    def expand_one_column_array_of_object(
        self, table: DbTable, expanded_column: ExpandedColumn
    ):
        schema = expanded_column.schema

        assert schema.ty == JsonSchemaTypes.Array
        assert schema.array_element_schema is not None
        assert schema.array_element_schema.ty == JsonSchemaTypes.Object

        assert schema.array_element_schema.fields is not None
        for field in schema.array_element_schema.fields:
            column = derive_column(expanded_column, ExpandOps.ArrayExpand, field.name)
            self.expand_add_column(table, column)
            if self.array_of_object_expand_exists and not field.always_exists:
                column = derive_column(
                    expanded_column, ExpandOps.ArrayExpandExists, field.name
                )
                self.expand_add_column(table, column)

    def expand_one_column_array_of_dict(
        self, table: DbTable, expanded_column: ExpandedColumn
    ):
        schema = expanded_column.schema
        assert schema.ty == JsonSchemaTypes.Array
        assert schema.array_element_schema is not None
        assert schema.array_element_schema.ty == JsonSchemaTypes.Dict

        key_column = derive_column(expanded_column, ExpandOps.ArrayDictKey, None)
        self.expand_add_column(table, key_column)

        value_column = derive_column(expanded_column, ExpandOps.ArrayDictValue, None)
        self.expand_add_column(table, value_column)

    def expand_one_column_dict(self, table: DbTable, expanded_column: ExpandedColumn):
        schema = expanded_column.schema
        assert schema.ty == JsonSchemaTypes.Dict
        assert schema.array_element_schema is not None

        key_column = derive_column(expanded_column, ExpandOps.DictKey, None)
        self.expand_add_column(table, key_column)

        value_column = derive_column(expanded_column, ExpandOps.DictValue, None)
        self.expand_add_column(table, value_column)
