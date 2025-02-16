import inspect
import typing
from dataclasses import dataclass


# Invariant
# Domain: [[event, fields], [event, fields], ...]
# Quantifier: 'forall' or 'exists'
# Premise: True, Nearest Sequence, Same Session
# Predicate: python program
class Invariant:
    domain: "list[APIDomain]"
    premise: "Premise"
    predicate: "Predicate"

    def __str__(self):
        return f"Domain: {self.domain}, Premise: {{ {self.premise} }}, Predicate: {{ {self.predicate} }}"

    @staticmethod
    def construct_from(
        domain: "list[APIDomain]", premise: "Premise", predicate: "Predicate"
    ) -> "Invariant":
        invariant = Invariant()
        invariant.domain = domain
        invariant.premise = premise
        invariant.predicate = predicate

        assert isinstance(invariant.domain, list)
        assert isinstance(invariant.premise, Premise)
        assert isinstance(invariant.predicate, Predicate)

        for d in invariant.domain:
            assert isinstance(d, APIDomain)

        return invariant

    def load_from_json(self, j: dict) -> None:
        self.domain = []
        for d in j["domain"]:
            domain = APIDomain()
            domain.load_from_json(d)
            self.domain.append(domain)

        self.premise = Premise()
        self.premise.load_from_json(j["premise"])

        self.predicate = Predicate()
        self.predicate.load_from_json(j["predicate"])

    def save_to_json(self) -> dict:
        domain = []
        for d in self.domain:
            domain.append(d.save_to_json())

        return {
            "domain": domain,
            "premise": self.premise.save_to_json(),
            "predicate": self.predicate.save_to_json(),
        }


class APIDomainAllPlaceholder:
    pass


API_DOMAIN_ALL_PLACEHOLDER = APIDomainAllPlaceholder()


@dataclass
class RelatedFields:
    include_arguments: bool = False
    include_response: bool = False
    include_headers: bool = False
    include_env: bool = False
    include_db_info: bool = False
    include_related_log: bool = False
    related_include_arguments: bool = False
    related_include_response: bool = False
    related_include_headers: bool = False
    related_include_env: bool = False

    def save_to_json(self) -> dict[str, bool]:
        return {
            "include_arguments": self.include_arguments,
            "include_response": self.include_response,
            "include_headers": self.include_headers,
            "include_env": self.include_env,
            "include_db_info": self.include_db_info,
            "include_related_log": self.include_related_log,
            "related_include_arguments": self.related_include_arguments,
            "related_include_response": self.related_include_response,
            "related_include_headers": self.related_include_headers,
            "related_include_env": self.related_include_env,
        }

    def load_from_json(self, j: dict[str, bool]) -> None:
        self.include_arguments = j["include_arguments"]
        self.include_response = j["include_response"]
        self.include_headers = j["include_headers"]
        self.include_env = j["include_env"]
        self.include_db_info = j["include_db_info"]
        self.include_related_log = j["include_related_log"]
        self.related_include_arguments = j["related_include_arguments"]
        self.related_include_response = j["related_include_response"]
        self.related_include_headers = j["related_include_headers"]
        self.related_include_env = j["related_include_env"]


class APIDomain:
    api: typing.Union[str, APIDomainAllPlaceholder]
    related_fields: "list[Field] | RelatedFields"
    quantifier: typing.Literal["forall", "exists"]

    def __str__(self):
        return f"API: {self.api}, Related Fields: {self.related_fields}, Quantifier: {self.quantifier}"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def construct_from(
        api: str,
        related_fields: "list[Field] | RelatedFields",
        quantifier: typing.Literal["forall", "exists"],
    ) -> "APIDomain":
        domain = APIDomain()
        domain.api = api
        domain.related_fields = related_fields
        domain.quantifier = quantifier

        assert isinstance(domain.api, str) or isinstance(
            domain.api, APIDomainAllPlaceholder
        )
        assert isinstance(domain.related_fields, list) or isinstance(
            domain.related_fields, RelatedFields
        )
        assert domain.quantifier in ["forall", "exists"]

        if isinstance(domain.related_fields, list):
            for f in domain.related_fields:
                assert isinstance(f, Field)

        return domain

    def load_from_json(self, j: dict) -> None:
        api = j["api"]
        if api == "__all__":
            self.api = API_DOMAIN_ALL_PLACEHOLDER
        else:
            self.api = api

        if isinstance(j["related_fields"], list):
            self.related_fields = []
            for f in j["related_fields"]:
                field = Field()
                field.load_from_json(f)
                self.related_fields.append(field)
        else:
            self.related_fields = RelatedFields()
            self.related_fields.load_from_json(j["related_fields"])

        self.quantifier = j["quantifier"]
        assert self.quantifier in ["forall", "exists"]

    def is_api_domain_all(self) -> bool:
        return isinstance(self.api, APIDomainAllPlaceholder)

    def save_to_json(self) -> dict:
        if isinstance(self.related_fields, list):
            related_fields = []
            for f in self.related_fields:
                related_fields.append(f.save_to_json())
        else:
            related_fields = self.related_fields.save_to_json()

        return {
            "api": "__all__" if self.is_api_domain_all() else self.api,
            "related_fields": related_fields,
            "quantifier": self.quantifier,
        }


# To show one reated variable in the invariant.
# category: the category of the variable, should be one of "argument", "response", "header", "env"
# name: the name of the variable
class Field:
    category: typing.Literal["argument", "response", "header", "env"]
    name: str

    @staticmethod
    def construct_from(category: str, name: str) -> "Field":
        field = Field()
        field.category = category
        field.name = name

        assert field.category in ["argument", "response", "header", "env"]

        return field

    def load_from_json(self, j: dict) -> None:
        self.category = j["category"]
        assert self.category in ["argument", "response", "header", "env"]

        self.name = j["name"]

    def save_to_json(self) -> dict:
        return {"category": self.category, "name": self.name}


class Premise:
    premise_position: typing.Literal["derive", "and"]
    premise_type: typing.Literal[
        "true", "two_second_nearest_after_first", "two_first_nearest_after_second"
    ]

    def __str__(self):
        return f"Premise Position: {self.premise_position}, Premise Type: {self.premise_type}"

    @staticmethod
    def construct_from(
        premise_position: typing.Literal["derive", "and"],
        premise_type: typing.Literal[
            "true", "two_second_nearest_after_first", "two_first_nearest_after_second"
        ],
    ) -> "Premise":
        premise = Premise()
        premise.premise_position = premise_position
        premise.premise_type = premise_type

        assert premise.premise_position in ["derive", "and"]
        assert premise.premise_type in [
            "true",
            "two_second_nearest_after_first",
            "two_first_nearest_after_second",
        ]

        return premise

    def load_from_json(self, j: dict) -> None:
        self.premise_position = j["premise_position"]
        assert self.premise_position in ["derive", "and"]

        self.premise_type = j["premise_type"]
        assert self.premise_type in [
            "true",
            "two_second_nearest_after_first",
            "two_first_nearest_after_second",
        ]

    def save_to_json(self) -> dict:
        return {
            "premise_position": self.premise_position,
            "premise_type": self.premise_type,
        }


def dummy_print(*args, **kwargs):
    pass


def check_valid_predicate_code(py_code: str, num_args):
    py_func_globabls = {"print": dummy_print}
    try:
        exec(py_code, py_func_globabls)
    except Exception as e:
        raise ValueError(f"Failed during exec Python code. {e}")
    if "check" not in py_func_globabls:
        raise ValueError("No 'check' function in the Python code.")
    py_func = py_func_globabls["check"]

    if not inspect.isfunction(py_func):
        raise ValueError("The 'check' is not a function.")

    signature = inspect.signature(py_func)
    for param_name, param in signature.parameters.items():
        if param.kind not in [
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
        ]:
            raise ValueError(f"Invalid parameter kind: {param_name} {param.kind}")

    if len(signature.parameters) != num_args:
        raise ValueError(
            f"Number of parameters in the 'check' function is not equal to {num_args}"
        )

    return py_func, py_func_globabls


class Predicate:
    is_true_predicate: bool
    desc: str
    py_code: str
    py_func: typing.Callable
    py_func_globabls: dict
    num_args: int

    def __str__(self):
        return f"Is True Predicate: {self.is_true_predicate}, Description: {self.desc}, Python Code: {self.py_code}, Number of Arguments: {self.num_args}"

    @staticmethod
    def construct_true_predicate() -> "Predicate":
        predicate = Predicate()
        predicate.is_true_predicate = True
        return predicate

    @staticmethod
    def construct_from_py_code(desc: str, py_code: str, num_args: int) -> "Predicate":
        predicate = Predicate()
        predicate.load_py_code(desc, py_code, num_args)
        return predicate

    def load_py_code(self, desc: str, py_code: str, num_args: int) -> None:
        assert isinstance(desc, str)
        assert isinstance(py_code, str)
        assert isinstance(num_args, int)
        assert num_args > 0

        self.is_true_predicate = False
        self.desc = desc
        self.py_code = py_code
        self.num_args = num_args

        py_func, py_func_globabls = check_valid_predicate_code(
            self.py_code, self.num_args
        )
        self.py_func = py_func
        self.py_func_globabls = py_func_globabls

    def load_from_json(self, j: dict) -> None:
        self.is_true_predicate = j["is_true_precisely"]

        if not self.is_true_predicate:
            self.load_py_code(j["desc"], j["py_code"], j["num_args"])

    def save_to_json(self) -> dict:
        if self.is_true_predicate:
            return {"is_true_precisely": self.is_true_predicate}
        else:
            return {
                "is_true_precisely": self.is_true_predicate,
                "desc": self.desc,
                "py_code": self.py_code,
                "num_args": self.num_args,
            }
