import json
from dataclasses import dataclass


class JsonSchemaTypes:
    Null = "null"

    Str = "str"
    Int = "int"
    Float = "float"
    Bool = "bool"
    Bytes = "bytes"

    Array = "array"
    Object = "object"

    Dict = "dict"

    Unknown = "unknown"


@dataclass
class JsonSchemaField:
    name: str
    always_exists: bool
    schema: "JsonSchema"

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "always_exists": self.always_exists,
            "schema": self.schema.to_json(),
        }

    @staticmethod
    def from_json(json: dict) -> "JsonSchemaField":
        return JsonSchemaField(
            name=json["name"],
            always_exists=json["always_exists"],
            schema=JsonSchema.from_json(json["schema"]),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JsonSchemaField):
            return False

        if self.name != other.name:
            return False

        if self.always_exists != other.always_exists:
            return False

        if self.schema != other.schema:
            return False

        return True

    def soft_eq(self, other: object) -> bool:
        if not isinstance(other, JsonSchemaField):
            return False

        if self.name != other.name:
            return False

        if not self.schema.soft_eq(other.schema):
            return False

        return True


class JsonSchemaJsonPlaceHolders:
    STR = "_json_schema_placeholder_str_"
    INT = "_json_schema_placeholder_int_"
    FLOAT = "_json_schema_placeholder_float_"
    BOOL = "_json_schema_placeholder_bool_"
    BYTES = "_json_schema_placeholder_bytes_"

    ARRAY_EXTRA = "_json_schema_placeholder_array_extra_"

    DICT_KEY = "_json_schema_placeholder_dict_key_"
    DICT_EXTRA = "_json_schema_placeholder_dict_extra_"
    DICT_EXTRA_VALUE = "_json_schema_placeholder_dict_value_"

    UNKNOWN = "_json_schema_placeholder_unknown_"


class JsonSchemaJsonPlaceHolderReplaces:
    STR = "<string value>"
    INT = "<int value>"
    FLOAT = "<float value>"
    BOOL = "<bool value>"
    BYTES = "<bytes value>"

    ARRAY_EXTRA = "<extra array elements . . .>"

    DICT_KEY = "<dict string key>"
    DICT_EXTRA = "<extra dict keys . . .>"
    DICT_EXTRA_VALUE = "<dict value>"

    UNKNOWN = "<unknown value>"


def _place_replacement_generator():
    replacing_pairs = []
    for key in JsonSchemaJsonPlaceHolders.__dict__:
        if key.startswith("_"):
            continue
        place_holder_value = getattr(JsonSchemaJsonPlaceHolders, key)
        place_holder_value = '"' + place_holder_value + '"'
        replace_value = getattr(JsonSchemaJsonPlaceHolderReplaces, key)

        replacing_pairs.append((place_holder_value, replace_value))

    return replacing_pairs


JSON_SCHEMA_REPLACEMENTS = _place_replacement_generator()


def replace_json_str_placeholders(json_str: str) -> str:
    for place_holder, replace in JSON_SCHEMA_REPLACEMENTS:
        json_str = json_str.replace(place_holder, replace)
    return json_str


def schema_json_to_string(j) -> str:
    return replace_json_str_placeholders(json.dumps(j, indent=2))


@dataclass
class JsonSchema:
    ty: str

    can_null: bool
    is_unique: bool

    len_min: int | None
    len_max: int | None

    value_min: float | int | None
    value_max: float | int | None

    fields: list[JsonSchemaField] | None
    array_element_schema: "JsonSchema | None"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JsonSchema):
            return False

        if self.ty != other.ty:
            return False

        if self.can_null != other.can_null:
            return False

        if self.is_unique != other.is_unique:
            return False

        if self.len_min != other.len_min:
            return False

        if self.len_max != other.len_max:
            return False

        if self.value_min != other.value_min:
            return False

        if self.value_max != other.value_max:
            return False

        if self.fields is None and other.fields is not None:
            return False

        if self.fields is not None and other.fields is None:
            return False

        if self.fields is not None and other.fields is not None:
            if len(self.fields) != len(other.fields):
                return False

            for field in self.fields:
                has_match = False
                for other_field in other.fields:
                    if field.name == other_field.name:
                        has_match = True
                        if field != other_field:
                            return False
                        break
                if not has_match:
                    return False

        if self.array_element_schema is None and other.array_element_schema is not None:
            return False

        if self.array_element_schema is not None and other.array_element_schema is None:
            return False

        if (
            self.array_element_schema is not None
            and other.array_element_schema is not None
        ):
            if self.array_element_schema != other.array_element_schema:
                return False

        return True

    def soft_eq(self, other: object) -> bool:
        if not isinstance(other, JsonSchema):
            return False

        if self.ty != other.ty:
            return False

        if self.fields is None and other.fields is not None:
            return False

        if self.fields is not None and other.fields is None:
            return False

        if self.fields is not None and other.fields is not None:
            if len(self.fields) != len(other.fields):
                return False

            for field in self.fields:
                has_match = False
                for other_field in other.fields:
                    if field.name == other_field.name:
                        has_match = True
                        if not field.soft_eq(other_field):
                            return False
                        break
                if not has_match:
                    return False

        if self.array_element_schema is None and other.array_element_schema is not None:
            return False

        if self.array_element_schema is not None and other.array_element_schema is None:
            return False

        if (
            self.array_element_schema is not None
            and other.array_element_schema is not None
        ):
            if not self.array_element_schema.soft_eq(other.array_element_schema):
                return False

        return True

    def to_json(self) -> dict:
        return {
            "ty": self.ty,
            "can_null": self.can_null,
            "is_unique": self.is_unique,
            "len_min": self.len_min,
            "len_max": self.len_max,
            "value_min": self.value_min,
            "value_max": self.value_max,
            "fields": (
                None if self.fields is None else [f.to_json() for f in self.fields]
            ),
            "array_element_schema": (
                None
                if self.array_element_schema is None
                else self.array_element_schema.to_json()
            ),
        }

    @staticmethod
    def from_json(json: dict) -> "JsonSchema":
        return JsonSchema(
            ty=json["ty"],
            can_null=json["can_null"],
            is_unique=json["is_unique"],
            len_min=json["len_min"],
            len_max=json["len_max"],
            value_min=json["value_min"],
            value_max=json["value_max"],
            fields=(
                None
                if json["fields"] is None
                else [JsonSchemaField.from_json(f) for f in json["fields"]]
            ),
            array_element_schema=(
                None
                if json["array_element_schema"] is None
                else JsonSchema.from_json(json["array_element_schema"])
            ),
        )

    def copy(self):
        return JsonSchema(
            ty=self.ty,
            can_null=self.can_null,
            is_unique=self.is_unique,
            len_min=self.len_min,
            len_max=self.len_max,
            value_min=self.value_min,
            value_max=self.value_max,
            fields=self.fields,
            array_element_schema=self.array_element_schema,
        )

    @staticmethod
    def new_unknown(can_null: bool) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Unknown,
            can_null=can_null,
            len_min=None,
            len_max=None,
            value_min=None,
            value_max=None,
            fields=None,
            array_element_schema=None,
            is_unique=False,
        )

    @staticmethod
    def new_null() -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Null,
            can_null=True,
            len_min=None,
            len_max=None,
            value_min=None,
            value_max=None,
            fields=None,
            array_element_schema=None,
            is_unique=True,
        )

    @staticmethod
    def new_str(
        can_null: bool, len_min: int | None, len_max: int | None, is_unique: bool
    ) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Str,
            can_null=can_null,
            len_min=len_min,
            len_max=len_max,
            value_min=None,
            value_max=None,
            fields=None,
            array_element_schema=None,
            is_unique=is_unique,
        )

    @staticmethod
    def new_int(
        can_null: bool, value_min: int, value_max: int, is_unique: bool
    ) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Int,
            can_null=can_null,
            len_min=None,
            len_max=None,
            value_min=value_min,
            value_max=value_max,
            fields=None,
            array_element_schema=None,
            is_unique=is_unique,
        )

    @staticmethod
    def new_float(
        can_null: bool, value_min: float, value_max: float, is_unique: bool
    ) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Float,
            can_null=can_null,
            len_min=None,
            len_max=None,
            value_min=value_min,
            value_max=value_max,
            fields=None,
            array_element_schema=None,
            is_unique=is_unique,
        )

    @staticmethod
    def new_bool(
        can_null: bool, value_min: int, value_max: int, is_unique: bool
    ) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Bool,
            can_null=can_null,
            len_min=None,
            len_max=None,
            value_min=value_min,
            value_max=value_max,
            fields=None,
            array_element_schema=None,
            is_unique=is_unique,
        )

    @staticmethod
    def new_bytes(can_null: bool, len_min: int, len_max: int) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Bytes,
            can_null=can_null,
            len_min=len_min,
            len_max=len_max,
            value_min=None,
            value_max=None,
            fields=None,
            array_element_schema=None,
            is_unique=False,
        )

    @staticmethod
    def new_array(
        can_null: bool, len_min: int, len_max: int, array_element_schema
    ) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Array,
            can_null=can_null,
            len_min=len_min,
            len_max=len_max,
            value_min=None,
            value_max=None,
            fields=None,
            array_element_schema=array_element_schema,
            is_unique=False,
        )

    @staticmethod
    def new_object(can_null: bool, fields: list[JsonSchemaField]) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Object,
            can_null=can_null,
            len_min=None,
            len_max=None,
            value_min=None,
            value_max=None,
            fields=fields,
            array_element_schema=None,
            is_unique=False,
        )

    @staticmethod
    def new_dict(
        can_null: bool, value_schema: "JsonSchema", len_min: int, len_max: int
    ) -> "JsonSchema":
        return JsonSchema(
            ty=JsonSchemaTypes.Dict,
            can_null=can_null,
            len_min=len_min,
            len_max=len_max,
            value_min=None,
            value_max=None,
            fields=None,
            array_element_schema=value_schema,
            is_unique=False,
        )

    def find_field(self, name: str) -> JsonSchemaField:
        assert self.fields is not None
        for field in self.fields:
            if field.name == name:
                return field
        raise ValueError(f"Field {name} not found in object schema")

    def is_primary(self) -> bool:
        return self.ty in [
            JsonSchemaTypes.Null,
            JsonSchemaTypes.Str,
            JsonSchemaTypes.Int,
            JsonSchemaTypes.Float,
            JsonSchemaTypes.Bool,
            JsonSchemaTypes.Bytes,
        ]

    def is_basic(self) -> bool:
        return self.is_primary() or (
            self.is_array() and self.array_element_schema.is_primary()  # type: ignore
        )

    def is_array(self) -> bool:
        return self.ty == JsonSchemaTypes.Array

    def basic_name(self) -> str:
        assert self.is_basic()
        if self.ty == JsonSchemaTypes.Null:
            return "null"
        if self.ty == JsonSchemaTypes.Str:
            return "str"
        if self.ty == JsonSchemaTypes.Int:
            return "int"
        if self.ty == JsonSchemaTypes.Float:
            return "float"
        if self.ty == JsonSchemaTypes.Bool:
            return "bool"
        if self.ty == JsonSchemaTypes.Bytes:
            return "bytes"
        if self.ty == JsonSchemaTypes.Array:
            return self.array_element_schema.basic_name() + "[]"  # type: ignore

        raise ValueError(f"Schema {self.ty} is not basic")

    def is_unk(self) -> bool:
        return self.ty == JsonSchemaTypes.Unknown or (
            self.ty == JsonSchemaTypes.Array
            and self.array_element_schema.is_unk()  # type: ignore
        )

    def to_schema_json(self):
        match self.ty:
            case JsonSchemaTypes.Null:
                return None
            case JsonSchemaTypes.Str:
                return JsonSchemaJsonPlaceHolders.STR
            case JsonSchemaTypes.Int:
                return JsonSchemaJsonPlaceHolders.INT
            case JsonSchemaTypes.Float:
                return JsonSchemaJsonPlaceHolders.FLOAT
            case JsonSchemaTypes.Bool:
                return JsonSchemaJsonPlaceHolders.BOOL
            case JsonSchemaTypes.Bytes:
                return JsonSchemaJsonPlaceHolders.BYTES
            case JsonSchemaTypes.Array:
                if self.len_max == 0:
                    return []
                element = self.array_element_schema.to_schema_json()
                if self.len_max == 1:
                    return [element]
                else:
                    return [element, JsonSchemaJsonPlaceHolders.ARRAY_EXTRA]
            case JsonSchemaTypes.Object:
                return {
                    field.name: field.schema.to_schema_json() for field in self.fields
                }
            case JsonSchemaTypes.Dict:
                element = self.array_element_schema.to_schema_json()
                return {
                    JsonSchemaJsonPlaceHolders.DICT_KEY: element,
                    JsonSchemaJsonPlaceHolders.DICT_EXTRA: JsonSchemaJsonPlaceHolders.DICT_EXTRA_VALUE,
                }
