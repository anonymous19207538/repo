from .db import DbValue, ExpandOp, ExpandOps
from .schema import JsonSchema, JsonSchemaTypes


def expand_map_with_none(value: DbValue, expand_op: ExpandOp) -> DbValue:
    if value is None:
        return None
    return expand_map(value, expand_op)


def expand_map(value: DbValue, expand_op: ExpandOp) -> DbValue:
    return EXPAND_DICT[expand_op.ty](value, expand_op.arg)


def expand_map_array_idx(value: DbValue, arg) -> DbValue:
    assert isinstance(arg, int)
    assert isinstance(value, list)

    if arg < len(value):
        return value[arg]
    else:
        return None


def expand_map_array_len(value: DbValue, arg) -> DbValue:
    assert isinstance(value, list)
    return len(value)


def expand_map_array_flatten(value: DbValue, arg) -> DbValue:
    assert isinstance(value, list)
    results = []
    for item in value:
        if item is None:
            results.append(None)
        else:
            assert isinstance(item, list)
            results.extend(item)
    return results


def expand_map_array_expand(value: DbValue, arg) -> DbValue:
    assert isinstance(value, list)
    assert isinstance(arg, str)

    result = []
    for item in value:
        if item is None:
            result.append(None)
        else:
            assert isinstance(item, dict)
            if arg in item:
                result.append(item[arg])
            else:
                result.append(None)
    return result


def expand_map_array_expand_exists(value: DbValue, arg) -> DbValue:
    assert isinstance(value, list)
    assert isinstance(arg, str)

    result = []
    for item in value:
        if item is None:
            result.append(None)
        else:
            assert isinstance(item, dict)
            result.append(arg in item)
    return result


def expand_map_object_expand(value: DbValue, arg) -> DbValue:
    assert isinstance(value, dict)
    assert isinstance(arg, str)

    if arg in value:
        return value[arg]
    else:
        return None


def expand_map_object_field_exists(value: DbValue, arg) -> DbValue:
    assert isinstance(value, dict)
    assert isinstance(arg, str)

    return arg in value


def expand_map_dict_key(value: DbValue, arg) -> DbValue:
    assert isinstance(value, dict)
    return list(value.keys())


def expand_map_dict_value(value: DbValue, arg) -> DbValue:
    assert isinstance(value, dict)
    return list(value.values())


def expand_map_array_dict_key(value: DbValue, arg) -> DbValue:
    assert isinstance(value, list)
    result = []
    for item in value:
        if item is None:
            result.append(None)
        else:
            assert isinstance(item, dict)
            result.append(list(item.keys()))
    return result


def expand_map_array_dict_value(value: DbValue, arg) -> DbValue:
    assert isinstance(value, list)
    result = []
    for item in value:
        if item is None:
            result.append(None)
        else:
            assert isinstance(item, dict)
            result.append(list(item.values()))
    return result


EXPAND_DICT = {
    ExpandOps.ArrayIdx: expand_map_array_idx,
    ExpandOps.ArrayLen: expand_map_array_len,
    ExpandOps.ArrayFlatten: expand_map_array_flatten,
    ExpandOps.ArrayExpand: expand_map_array_expand,
    ExpandOps.ArrayExpandExists: expand_map_array_expand_exists,
    ExpandOps.ObjectExpand: expand_map_object_expand,
    ExpandOps.ObjectFieldExists: expand_map_object_field_exists,
    ExpandOps.DictKey: expand_map_dict_key,
    ExpandOps.DictValue: expand_map_dict_value,
    ExpandOps.ArrayDictKey: expand_map_array_dict_key,
    ExpandOps.ArrayDictValue: expand_map_array_dict_value,
}


def expand_name(expand_op: ExpandOp) -> str:
    return EXPAND_NAME_DICT[expand_op.ty](expand_op, expand_op.arg)


def expand_name_array_idx(expand_op: ExpandOp, arg) -> str:
    assert isinstance(arg, int)
    return f"[{arg}]"


def expand_name_array_len(expand_op: ExpandOp, arg) -> str:
    return "#length"


def expand_name_array_flatten(expand_op: ExpandOp, arg) -> str:
    return "#flatten"


def expand_name_array_expand(expand_op: ExpandOp, arg) -> str:
    assert isinstance(arg, str)
    return f"[].{arg}"


def expand_name_array_expand_exists(expand_op: ExpandOp, arg) -> str:
    assert isinstance(arg, str)
    return f"[].{arg}#exists"


def expand_name_object_expand(expand_op: ExpandOp, arg) -> str:
    assert isinstance(arg, str)
    return f".{arg}"


def expand_name_object_field_exists(expand_op: ExpandOp, arg) -> str:
    assert isinstance(arg, str)
    return f".{arg}#exists"


def expand_name_dict_key(expand_op: ExpandOp, arg) -> str:
    return "#key"


def expand_name_dict_value(expand_op: ExpandOp, arg) -> str:
    return "#value"


def expand_name_array_dict_key(expand_op: ExpandOp, arg) -> str:
    return "[]#key"


def expand_name_array_dict_value(expand_op: ExpandOp, arg) -> str:
    return "[]#value"


EXPAND_NAME_DICT = {
    ExpandOps.ArrayIdx: expand_name_array_idx,
    ExpandOps.ArrayLen: expand_name_array_len,
    ExpandOps.ArrayFlatten: expand_name_array_flatten,
    ExpandOps.ArrayExpand: expand_name_array_expand,
    ExpandOps.ArrayExpandExists: expand_name_array_expand_exists,
    ExpandOps.ObjectExpand: expand_name_object_expand,
    ExpandOps.ObjectFieldExists: expand_name_object_field_exists,
    ExpandOps.DictKey: expand_name_dict_key,
    ExpandOps.DictValue: expand_name_dict_value,
    ExpandOps.ArrayDictKey: expand_name_array_dict_key,
    ExpandOps.ArrayDictValue: expand_name_array_dict_value,
}


def schema_copy_or_can_null(schema: JsonSchema, can_null: bool) -> JsonSchema:
    return JsonSchema(
        ty=schema.ty,
        can_null=schema.can_null or can_null,
        is_unique=schema.is_unique,
        len_min=schema.len_min,
        len_max=schema.len_max,
        value_min=schema.value_min,
        value_max=schema.value_max,
        fields=schema.fields,
        array_element_schema=schema.array_element_schema,
    )


def schema_copy_replace_min_max(schema: JsonSchema, len_min: int, len_max: int):
    return JsonSchema(
        ty=schema.ty,
        can_null=schema.can_null,
        is_unique=False,
        len_min=len_min,
        len_max=len_max,
        value_min=schema.value_min,
        value_max=schema.value_max,
        fields=schema.fields,
        array_element_schema=schema.array_element_schema,
    )


def expand_schema(schema: JsonSchema, expand_op: ExpandOp) -> JsonSchema:
    return EXPAND_SCHEMA_DICT[expand_op.ty](schema, expand_op.arg)


def expand_schema_array_idx(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Array
    assert schema.array_element_schema is not None

    assert isinstance(arg, int)

    return schema_copy_or_can_null(schema.array_element_schema, schema.can_null)


def expand_schema_array_len(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Array
    return JsonSchema(
        ty=JsonSchemaTypes.Int,
        can_null=schema.can_null,
        is_unique=False,
        len_min=None,
        len_max=None,
        value_min=schema.len_min,
        value_max=schema.len_max,
        fields=None,
        array_element_schema=None,
    )


def expand_schema_array_flatten(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Array
    assert schema.array_element_schema is not None
    assert schema.array_element_schema.ty == JsonSchemaTypes.Array

    assert schema.len_min is not None
    assert schema.len_max is not None
    assert schema.array_element_schema.len_min is not None
    assert schema.array_element_schema.len_max is not None

    return schema_copy_replace_min_max(
        schema.array_element_schema,
        (
            (schema.len_min * schema.array_element_schema.len_min)
            if not schema.array_element_schema.can_null
            else 0
        ),
        schema.len_max * schema.array_element_schema.len_max,
    )


def expand_schema_array_expand(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Array
    assert schema.len_min is not None
    assert schema.len_max is not None
    assert schema.array_element_schema is not None
    assert schema.array_element_schema.ty == JsonSchemaTypes.Object
    assert schema.array_element_schema.fields is not None

    assert isinstance(arg, str)

    field_target = schema.array_element_schema.find_field(arg)
    return JsonSchema.new_array(
        can_null=schema.can_null,
        len_min=schema.len_min,
        len_max=schema.len_max,
        array_element_schema=field_target.schema,
    )


def expand_schema_array_expand_exists(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Array
    assert schema.len_min is not None
    assert schema.len_max is not None
    assert schema.array_element_schema is not None
    assert schema.array_element_schema.ty == JsonSchemaTypes.Object
    assert schema.array_element_schema.fields is not None

    assert isinstance(arg, str)

    return JsonSchema.new_array(
        can_null=schema.can_null,
        len_min=schema.len_min,
        len_max=schema.len_max,
        array_element_schema=JsonSchema.new_bool(
            can_null=schema.can_null, value_min=0, value_max=1, is_unique=False
        ),
    )


def expand_schema_object_expand(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Object
    assert schema.fields is not None

    assert isinstance(arg, str)

    field_target = schema.find_field(arg)
    return schema_copy_or_can_null(field_target.schema, schema.can_null)


def expand_schema_object_field_exists(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Object
    assert schema.fields is not None

    assert isinstance(arg, str)

    return JsonSchema.new_bool(
        can_null=schema.can_null, value_min=0, value_max=1, is_unique=False
    )


def expand_schema_dict_key(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Dict
    assert schema.len_min is not None
    assert schema.len_max is not None

    return JsonSchema.new_array(
        can_null=schema.can_null,
        len_min=schema.len_min,
        len_max=schema.len_max,
        array_element_schema=JsonSchema.new_str(
            can_null=schema.can_null, len_min=None, len_max=None, is_unique=True
        ),
    )


def expand_schema_dict_value(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Dict
    assert schema.len_min is not None
    assert schema.len_max is not None
    assert schema.array_element_schema is not None

    return JsonSchema.new_array(
        can_null=schema.can_null,
        len_min=schema.len_min,
        len_max=schema.len_max,
        array_element_schema=schema.array_element_schema,
    )


def expand_schema_array_dict_key(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Array
    assert schema.len_min is not None
    assert schema.len_max is not None
    assert schema.array_element_schema is not None
    assert schema.array_element_schema.ty == JsonSchemaTypes.Dict
    assert schema.array_element_schema.fields is not None
    assert schema.array_element_schema.len_min is not None
    assert schema.array_element_schema.len_max is not None

    return JsonSchema.new_array(
        can_null=schema.can_null,
        len_min=schema.len_min,
        len_max=schema.len_max,
        array_element_schema=JsonSchema.new_array(
            can_null=schema.can_null,
            len_min=schema.array_element_schema.len_min,
            len_max=schema.array_element_schema.len_max,
            array_element_schema=JsonSchema.new_str(
                can_null=schema.can_null, len_min=None, len_max=None, is_unique=True
            ),
        ),
    )


def expand_schema_array_dict_value(schema: JsonSchema, arg) -> JsonSchema:
    assert schema.ty == JsonSchemaTypes.Array
    assert schema.len_min is not None
    assert schema.len_max is not None
    assert schema.array_element_schema is not None
    assert schema.array_element_schema.ty == JsonSchemaTypes.Dict
    assert schema.array_element_schema.fields is not None
    assert schema.array_element_schema.len_min is not None
    assert schema.array_element_schema.len_max is not None
    assert schema.array_element_schema.array_element_schema is not None

    return JsonSchema.new_array(
        can_null=schema.can_null,
        len_min=schema.len_min,
        len_max=schema.len_max,
        array_element_schema=JsonSchema.new_array(
            can_null=schema.can_null,
            len_min=schema.array_element_schema.len_min,
            len_max=schema.array_element_schema.len_max,
            array_element_schema=schema.array_element_schema.array_element_schema,
        ),
    )


EXPAND_SCHEMA_DICT = {
    ExpandOps.ArrayIdx: expand_schema_array_idx,
    ExpandOps.ArrayLen: expand_schema_array_len,
    ExpandOps.ArrayFlatten: expand_schema_array_flatten,
    ExpandOps.ArrayExpand: expand_schema_array_expand,
    ExpandOps.ArrayExpandExists: expand_schema_array_expand_exists,
    ExpandOps.ObjectExpand: expand_schema_object_expand,
    ExpandOps.ObjectFieldExists: expand_schema_object_field_exists,
    ExpandOps.DictKey: expand_schema_dict_key,
    ExpandOps.DictValue: expand_schema_dict_value,
    ExpandOps.ArrayDictKey: expand_schema_array_dict_key,
    ExpandOps.ArrayDictValue: expand_schema_array_dict_value,
}
