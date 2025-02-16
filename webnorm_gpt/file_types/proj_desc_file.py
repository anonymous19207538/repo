import gzip
import json
import typing

import zstandard


class APIDesc:
    name: str
    def_req: typing.Optional[str]
    def_resp: typing.Optional[str]
    src: typing.Optional[str]
    extra: dict[str, typing.Any]
    argument_names: typing.Optional[list[str]]

    def __init__(self):
        pass

    def load_from_json(self, j: dict):
        self.name = j["name"]
        self.def_req = j.get("def_req", None)
        self.def_resp = j.get("def_resp", None)
        self.src = j.get("src", None)
        self.argument_names = j.get("argument_names", None)

        current_names = {"name", "def_req", "def_resp", "src", "argument_names"}
        self.extra = {}
        for k, v in j.items():
            if k not in current_names:
                self.extra[k] = v

    def to_json(self):
        return {
            "name": self.name,
            "def_req": self.def_req,
            "def_resp": self.def_resp,
            "src": self.src,
            "argument_names": self.argument_names,
            **self.extra,
        }


# A project description file should be a json file.
# It should have
# - name: str (the name of the project)
# - apis: list[APIDesc] (the list of api descriptions)
#     - name: str (the name of the api)
#     - def_req: Optional[str] (the definition of the request)
#     - def_resp: Optional[str] (the definition of the response)
#     - src: Optional[str] (the source code of the api)
# - relations_data: Optional[list[tuple[str, str]]] (the list of data dependency relations between the apis)
# - relations_control: Optional[list[tuple[str, str]]] (the list of control dependency relations between the apis)
class ProjDescFile:
    name: str
    apis: list[APIDesc]
    relations_data: typing.Optional[list[tuple[str, str]]]
    relations_control: typing.Optional[list[tuple[str, str]]]

    api_map: dict[str, APIDesc]

    def __init__(self):
        self.name = ""
        self.apis = []
        self.relations_data = []
        self.relations_control = []

    def load_from_file_path(self, file_path: str):
        if file_path.endswith(".gz"):
            open_func = gzip.open
        elif file_path.endswith(".zst"):
            open_func = zstandard.open
        else:
            open_func = open
        with open_func(file_path, "r") as f:
            j = json.load(f)
        self.name = j["name"]
        self.apis = []
        for api in j["apis"]:
            api_desc = APIDesc()
            api_desc.load_from_json(api)
            self.apis.append(api_desc)
        self.relations_data = j.get("relations_data", None)
        self.relations_control = j.get("relations_control", None)

        self.api_map = {api.name: api for api in self.apis}

    def save_to_path(self, file_path: str):
        if file_path.endswith(".gz"):
            open_func = gzip.open
        elif file_path.endswith(".zst"):
            open_func = zstandard.open
        else:
            open_func = open
        with open_func(file_path, "w") as f:
            j = {
                "name": self.name,
                "apis": [api.to_json() for api in self.apis],
                "relations_data": self.relations_data,
                "relations_control": self.relations_control,
            }
            json.dump(j, f, indent=2)
