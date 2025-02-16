from .db import DbValue
from .schema import JsonSchema, JsonSchemaField


def _max_can_none(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def _min_can_none(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


class JsonSchemaInducer:
    def __init__(
        self, num_non_always_exists_field_max: int = 5, num_max_fields: int = 30
    ):
        self.num_non_always_exists_field_max = num_non_always_exists_field_max
        self.num_max_fields = num_max_fields

    def induce_json_schema(self, data: list[DbValue]) -> JsonSchema:
        if len(data) == 0:
            raise ValueError("Cannot induce schema from empty data")

        has_null = False
        has_str = False
        has_int = False
        has_float = False
        has_bool = False
        has_array = False
        has_object = False
        has_bytes = False

        arr_min_len = None
        arr_max_len = None

        str_min_len = None
        str_max_len = None

        bytes_min_len = None
        bytes_max_len = None

        float_min = None
        float_max = None

        has_true = False
        has_flase = False

        set_str = set()
        has_dup_str = False
        set_int = set()
        has_dup_int = False
        set_float = set()
        has_dup_float = False

        for item in data:
            if item is None:
                has_null = True
            elif isinstance(item, str):
                has_str = True
                str_min_len = _min_can_none(str_min_len, len(item))
                str_max_len = _max_can_none(str_max_len, len(item))
                if item in set_str:
                    has_dup_str = True
                set_str.add(item)
            elif isinstance(item, int):
                has_int = True
                float_min = _min_can_none(float_min, item)
                float_max = _max_can_none(float_max, item)
                if item in set_int:
                    has_dup_int = True
                item_float = float(item)
                if item_float in set_float:
                    has_dup_float = True
                set_float.add(item_float)
            elif isinstance(item, float):
                has_float = True
                float_min = _min_can_none(float_min, item)
                float_max = _max_can_none(float_max, item)
                if item in set_float:
                    has_dup_float = True
                set_float.add(item)
            elif isinstance(item, bool):
                has_bool = True
                if item:
                    has_true = True
                else:
                    has_flase = True
            elif isinstance(item, list):
                has_array = True
                arr_min_len = _min_can_none(arr_min_len, len(item))
                arr_max_len = _max_can_none(arr_max_len, len(item))
            elif isinstance(item, dict):
                has_object = True
                arr_min_len = _min_can_none(arr_min_len, len(item))
                arr_max_len = _max_can_none(arr_max_len, len(item))
            elif isinstance(item, bytes):
                has_bytes = True
                bytes_min_len = _min_can_none(bytes_min_len, len(item))
                bytes_max_len = _max_can_none(bytes_max_len, len(item))
            else:
                raise ValueError(f"Unknown type {type(item)}, with value {item}")

        conflict_pairs = [
            (has_str, has_int),
            (has_str, has_float),
            (has_str, has_bool),
            (has_str, has_array),
            (has_str, has_object),
            (has_int, has_bool),
            (has_int, has_array),
            (has_int, has_object),
            (has_float, has_bool),
            (has_float, has_array),
            (has_float, has_object),
            (has_bool, has_array),
            (has_bool, has_object),
            (has_array, has_object),
            (has_bytes, has_str),
            (has_bytes, has_int),
            (has_bytes, has_float),
            (has_bytes, has_bool),
            (has_bytes, has_array),
            (has_bytes, has_object),
        ]
        if any([a and b for a, b in conflict_pairs]):
            return JsonSchema.new_unknown(can_null=has_null)

        if has_str:
            assert str_min_len is not None
            assert str_max_len is not None
            return JsonSchema.new_str(
                can_null=has_null,
                len_min=str_min_len,
                len_max=str_max_len,
                is_unique=not has_dup_str,
            )
        if has_bytes:
            assert bytes_min_len is not None
            assert bytes_max_len is not None
            return JsonSchema.new_bytes(
                can_null=has_null, len_min=bytes_min_len, len_max=bytes_max_len
            )
        if has_float:
            assert float_min is not None
            assert float_max is not None
            return JsonSchema.new_float(
                can_null=has_null,
                value_min=float_min,
                value_max=float_max,
                is_unique=not has_dup_float,
            )
        if has_int:
            assert float_min is not None
            assert float_max is not None
            assert isinstance(float_min, int)
            assert isinstance(float_max, int)
            return JsonSchema.new_int(
                can_null=has_null,
                value_min=float_min,
                value_max=float_max,
                is_unique=not has_dup_int,
            )
        if has_bool:
            return JsonSchema.new_bool(
                can_null=has_null,
                value_min=(0 if has_flase else 1),
                value_max=(1 if has_true else 0),
                is_unique=(not has_flase) or (not has_true),
            )
        if has_array:
            array_elements = []
            for item in data:
                if item is not None:
                    assert isinstance(item, list)
                    array_elements.extend(item)
            if len(array_elements) != 0:
                array_element_schema = self.induce_json_schema(array_elements)
            else:
                array_element_schema = JsonSchema.new_unknown(can_null=False)
            assert arr_min_len is not None
            assert arr_max_len is not None
            return JsonSchema.new_array(
                can_null=has_null,
                len_min=arr_min_len,
                len_max=arr_max_len,
                array_element_schema=array_element_schema,
            )
        if has_object:
            fields = set()
            for item in data:
                if item is not None:
                    assert isinstance(item, dict)
                    fields.update(item.keys())

            fields_always_exists = [True] * len(fields)
            for item in data:
                for i, field in enumerate(fields):
                    if item is not None:
                        if field not in item:
                            fields_always_exists[i] = False

            num_not_always_exists = sum([not x for x in fields_always_exists])

            if (
                num_not_always_exists <= self.num_non_always_exists_field_max
                and len(fields) <= self.num_max_fields
            ):
                fields = sorted(fields)
                fields_schema = []

                for field in fields:
                    field_data = []
                    always_exists = True
                    for item in data:
                        if item is not None:
                            if field in item:
                                assert isinstance(item, dict)
                                field_data.append(item[field])
                            else:
                                always_exists = False
                    field_schema = self.induce_json_schema(field_data)
                    fields_schema.append(
                        JsonSchemaField(field, always_exists, field_schema)
                    )

                return JsonSchema.new_object(can_null=has_null, fields=fields_schema)
            else:
                assert arr_min_len is not None
                assert arr_max_len is not None
                all_value_data = []
                for item in data:
                    if item is not None:
                        assert isinstance(item, dict)
                        all_value_data.extend(item.values())
                value_schema = self.induce_json_schema(all_value_data)
                return JsonSchema.new_dict(
                    can_null=has_null,
                    value_schema=value_schema,
                    len_min=arr_min_len,
                    len_max=arr_max_len,
                )

        if has_null:
            return JsonSchema.new_null()

        raise ValueError("No schema type found")
