import collections
import json
import unittest
import urllib.parse
from typing import Optional, Tuple


class JavaTypeAstNode:
    def get_related_tys(self):
        return None

    def get_readable_ty(self):
        return None

    def get_readable_ty_short(self):
        return None


class JavaTypeAstNodePrimitive(JavaTypeAstNode):
    ty: str
    readable_ty: str

    def __init__(self, ty: str, readable_ty: str):
        self.ty = ty
        self.readable_ty = readable_ty

    def get_related_tys(self):
        return []

    def get_readable_ty(self):
        return self.readable_ty

    def get_readable_ty_short(self):
        return self.readable_ty


PRIMITIVE_MAPPING = {
    "B": "byte",
    "C": "char",
    "D": "double",
    "F": "float",
    "I": "int",
    "J": "long",
    "S": "short",
    "Z": "boolean",
    "V": "void",
    "*": "*",
}

PRIMITIVE_MAPPING_TY = {
    k: JavaTypeAstNodePrimitive(k, v) for k, v in PRIMITIVE_MAPPING.items()
}


class JavaTypeAstNodeArray(JavaTypeAstNode):
    element_ty: JavaTypeAstNode

    def __init__(self, element_ty: JavaTypeAstNode):
        self.element_ty = element_ty

    def get_related_tys(self):
        return self.element_ty.get_related_tys()

    def get_readable_ty(self):
        return f"{self.element_ty.get_readable_ty()}[]"

    def get_readable_ty_short(self):
        return f"{self.element_ty.get_readable_ty_short()}[]"


class JavaTypeAstNodeClass(JavaTypeAstNode):
    class_name: str
    generics: list[JavaTypeAstNode]

    def __init__(self, class_name: str, generics: list[JavaTypeAstNode]):
        self.class_name = class_name
        self.generics = generics

    def get_related_tys(self):
        results = [self.class_name]
        for generic in self.generics:
            results.extend(generic.get_related_tys())
        return results

    def get_readable_ty(self):
        cls_name = self.class_name.replace("/", ".")
        if len(self.generics) > 0:
            generics = ", ".join(generic.get_readable_ty() for generic in self.generics)
            return f"{cls_name}<{generics}>"
        else:
            return cls_name

    def get_readable_ty_short(self):
        cls_name = self.class_name.split("/")[-1]
        if len(self.generics) > 0:
            generics = ", ".join(
                generic.get_readable_ty_short() for generic in self.generics
            )
            return f"{cls_name}<{generics}>"
        else:
            return cls_name


class JavaTypeAstParser:
    @staticmethod
    def parse_type(ty: str, loc: int) -> Tuple[JavaTypeAstNode, int]:
        first_ch = ty[loc]
        if first_ch in PRIMITIVE_MAPPING:
            return PRIMITIVE_MAPPING_TY[first_ch], loc + 1
        elif first_ch == "L":
            end1 = ty.find(";", loc)
            end2 = ty.find("<", loc)
            if end2 < 0 or end2 > end1:
                end = end1
                class_name = ty[loc + 1 : end]
                return JavaTypeAstNodeClass(class_name, []), end + 1
            else:
                end = end2
                class_name = ty[loc + 1 : end]
                generic_list, new_loc = JavaTypeAstParser.parse_generic_list(ty, end)
                assert ty[new_loc] == ";"
                new_loc += 1
                return JavaTypeAstNodeClass(class_name, generic_list), new_loc
        elif first_ch == "[":
            element_ty, new_loc = JavaTypeAstParser.parse_type(ty, loc + 1)
            return JavaTypeAstNodeArray(element_ty), new_loc
        elif first_ch == "T":
            end = ty.find(";", loc)
            # FIXME: currently, we do not consider generic types
            class_name = "java/lang/Object"
            return JavaTypeAstNodeClass(class_name, []), end + 1
        elif first_ch == "+":
            loc += 1
            return JavaTypeAstParser.parse_type(ty, loc)
        else:
            raise ValueError(f"Unknown type {ty[loc:]} inside {ty}")

    def parse_generic_list(ty: str, loc: int) -> Tuple[list[JavaTypeAstNode], int]:
        assert ty[loc] == "<"
        loc += 1
        results = []
        while ty[loc] != ">":
            ty_node, new_loc = JavaTypeAstParser.parse_type(ty, loc)
            results.append(ty_node)
            loc = new_loc
        return results, loc + 1

    @staticmethod
    def parse_func_ty(ty: str) -> Tuple[list[JavaTypeAstNode], JavaTypeAstNode]:
        assert ty[0] == "("
        loc = 1
        param_tys = []
        while ty[loc] != ")":
            param_ty, new_loc = JavaTypeAstParser.parse_type(ty, loc)
            param_tys.append(param_ty)
            loc = new_loc
        ret_ty, new_loc = JavaTypeAstParser.parse_type(ty, loc + 1)
        assert new_loc == len(ty)
        return param_tys, ret_ty

    @staticmethod
    def parse_ty_str(tystr: str) -> JavaTypeAstNode:
        ty, loc = JavaTypeAstParser.parse_type(tystr, 0)
        assert loc == len(tystr)
        return ty


class JavaFieldInfo:
    name: str
    ty: str

    readable_ty: str
    readable_ty_short: str
    related_ty: list[str]

    def __init__(self, name: str, ty: str):
        self.name = name
        self.ty = ty

    def process(self, package_info: "JavaPackageInfo"):
        ty_ast = JavaTypeAstParser.parse_ty_str(self.ty)
        self.readable_ty = ty_ast.get_readable_ty()
        self.readable_ty_short = ty_ast.get_readable_ty_short()
        related_ty = ty_ast.get_related_tys()
        self.related_ty = related_ty


class JavaMethodInfo:
    name: str
    ty: str
    param_names: list[str]

    readable_tys: list[str]
    readable_tys_short: list[str]
    related_tys: list[list[str]]
    readable_ret_ty: str
    readable_ret_ty_short: str
    related_ret_ty: list[str]

    annos: list[dict]
    invoke: list[tuple[str, str, str]]

    def __init__(self, name: str, ty: str):
        self.name = name
        self.ty = ty
        self.param_names = []

        self.annos = []
        self.invoke = []

    def process(self, package_info: "JavaPackageInfo"):
        param_tys, ret_ty = JavaTypeAstParser.parse_func_ty(self.ty)
        self.readable_tys = [ty.get_readable_ty() for ty in param_tys]
        self.readable_tys_short = [ty.get_readable_ty_short() for ty in param_tys]
        self.related_tys = []
        for ty in param_tys:
            related_ty = ty.get_related_tys()
            self.related_tys.append(related_ty)
        self.readable_ret_ty = ret_ty.get_readable_ty()
        self.readable_ret_ty_short = ret_ty.get_readable_ty_short()
        ret_related_ty = ret_ty.get_related_tys()
        self.related_ret_ty = ret_related_ty

    def gen_readable_type_info_params_like_java(
        self,
        appending: list[str],
        package_info: "JavaPackageInfo",
        depth_remaining: int = 5,
        short=True,
    ):
        related_tys = set()
        related_tys_list = list()
        for i, (ty, ty_short, related_ty, name) in enumerate(
            zip(
                self.readable_tys,
                self.readable_tys_short,
                self.related_tys,
                self.param_names,
            )
        ):
            if i != 0:
                appending.append(", ")
            appending.append(ty_short if short else ty)
            appending.append(" ")
            appending.append(name)
            for r in related_ty:
                if r not in related_tys:
                    related_tys_list.append(r)
                    related_tys.add(r)
        appending.append("\n")
        package_info.gen_related_ty_info_like_java(
            appending, related_tys, depth=depth_remaining, short=short
        )

    def gen_readable_type_info_params(
        self,
        appending: list[str],
        package_info: "JavaPackageInfo",
        depth_remaining: int = 5,
        short=True,
    ):
        for i, (ty, ty_short, related_ty, name) in enumerate(
            zip(
                self.readable_tys,
                self.readable_tys_short,
                self.related_tys,
                self.param_names,
            )
        ):
            if i != 0:
                appending.append(", ")
            appending.append(ty_short if short else ty)
            appending.append(" ")
            appending.append(name)
            package_info.gen_related_ty_info(
                appending, related_ty, depth_remaining, short
            )

    def gen_readable_type_info_ret_like_java(
        self,
        appending: list[str],
        package_info: "JavaPackageInfo",
        depth_remaining: int = 5,
        short=True,
    ):
        appending.append(self.readable_ret_ty_short if short else self.readable_ret_ty)
        appending.append("\n")
        package_info.gen_related_ty_info_like_java(
            appending, self.related_ret_ty, depth_remaining, short
        )

    def gen_readable_type_info_ret(
        self,
        appending: list[str],
        package_info: "JavaPackageInfo",
        depth_remaining: int = 5,
        short=True,
    ):
        appending.append(self.readable_ret_ty_short if short else self.readable_ret_ty)
        package_info.gen_related_ty_info(
            appending, self.related_ret_ty, depth_remaining, short
        )


class JavaClassInfo:
    name: str
    is_enum: bool
    is_interface: bool
    is_abstract: bool

    fields: list[JavaFieldInfo]
    static_fields: list[JavaFieldInfo]

    methods: list[JavaMethodInfo]

    readable_name: str
    readable_name_short: str

    super: str
    impls: list[str]

    annos: list[dict]

    def __init__(self, name: str, is_enum: bool, is_interface: bool, is_abstract: bool):
        self.name = name
        self.is_enum = is_enum
        self.is_interface = is_interface
        self.is_abstract = is_abstract

        self.fields = []
        self.static_fields = []

        self.methods = []

        self.super = None
        self.impls = []

        self.annos = []

    def find_method(self, method_name: str) -> Optional[JavaMethodInfo]:
        for method in self.methods:
            if method.name == method_name:
                return method
        return None

    def process(self, package_info: "JavaPackageInfo"):
        for field in self.fields:
            field.process(package_info)
        for field in self.static_fields:
            field.process(package_info)
        for method in self.methods:
            method.process(package_info)
        self.readable_name = self.name.replace("/", ".")
        self.readable_name_short = self.readable_name.split(".")[-1]

    def gen_readable_type_info_like_java(
        self,
        appending: list[str],
        package_info: "JavaPackageInfo",
        depth_remaining: int = 5,
        short=True,
    ):
        related_tys = set()
        related_tys_list = list()
        if self.is_enum:
            has_something = False
            appending.append(" { ")
            for i, field in enumerate(self.static_fields):
                if field.readable_ty == self.readable_name:
                    if has_something:
                        appending.append(", ")
                    appending.append(f"{field.name}")
                    has_something = True
            appending.append(" }")
        else:
            appending.append(" {\n")
            for i, field in enumerate(self.fields):
                appending.append("    ")
                appending.append(
                    field.readable_ty_short if short else field.readable_ty
                )
                appending.append(" ")
                appending.append(field.name)
                appending.append(";\n")

                for r in field.related_ty:
                    if r not in related_tys:
                        related_tys_list.append(r)
                        related_tys.add(r)

            appending.append("}")

        return related_tys_list

    def gen_readable_type_info(
        self,
        appending: list[str],
        package_info: "JavaPackageInfo",
        depth_remaining: int = 5,
        short=True,
    ):
        if self.is_enum:
            appending.append(" { ")
            for i, field in enumerate(self.static_fields):
                if i != 0:
                    appending.append(", ")
                if field.readable_ty == self.readable_name:
                    appending.append(f"{field.name}")
            appending.append(" }")
        else:
            appending.append(" { ")
            for i, field in enumerate(self.fields):
                if i != 0:
                    appending.append(", ")
                appending.append(
                    field.readable_ty_short if short else field.readable_ty
                )
                appending.append(" ")
                appending.append(field.name)
                package_info.gen_related_ty_info(
                    appending, field.related_ty, depth_remaining, short
                )
            appending.append(" }")


class JavaPackageInfo:
    classes: dict[str, JavaClassInfo]

    IGNORE_CONTENT_LIST = {
        "java/lang/String",
        "java/lang/Byte",
        "java/lang/Char",
        "java/lang/Double",
        "java/lang/Float",
        "java/lang/Integer",
        "java/lang/Long",
        "java/lang/Short",
        "java/lang/Boolean",
        "java/lang/Void",
        "java/lang/Object",
    }

    def __init__(self):
        self.classes = {}

    def process(self):
        for class_info in self.classes.values():
            class_info.process(self)

    def gen_related_ty_info_like_java(
        self, appending: list[str], start_tys: list[str], depth: int = 5, short=True
    ):
        generated_cls = set()
        q = collections.deque()
        for root in start_tys:
            q.append((depth, root))

            while len(q) > 0:
                depth_remaining, cur = q.pop()
                if cur in generated_cls:
                    continue
                generated_cls.add(cur)
                if cur in JavaPackageInfo.IGNORE_CONTENT_LIST:
                    continue
                if cur not in self.classes:
                    continue
                cls_name_show = cur.split("/")[-1] if short else cur.replace("/", ".")
                is_enum = self.classes[cur].is_enum
                if is_enum:
                    appending.append("\n\nenum ")
                else:
                    appending.append("\n\nclass ")
                appending.append(cls_name_show)
                related_tys = self.classes[cur].gen_readable_type_info_like_java(
                    appending, self, depth_remaining, short
                )
                if depth_remaining > 0:
                    for r in related_tys[::-1]:
                        q.append((depth_remaining - 1, r))

    def gen_related_ty_info(
        self, appending: list[str], cls_names: list[str], depth: int = 5, short=True
    ):
        depth -= 1
        if depth <= 0:
            return
        has_non_external = False
        for cls_name in cls_names:
            if cls_name in JavaPackageInfo.IGNORE_CONTENT_LIST:
                continue
            elif cls_name in self.classes:
                has_non_external = True
        for cls_name in cls_names:
            if cls_name in JavaPackageInfo.IGNORE_CONTENT_LIST:
                continue
            elif cls_name in self.classes:
                cls_name_show = (
                    cls_name.split("/")[-1] if short else cls_name.replace("/", ".")
                )

                is_enum = self.classes[cls_name].is_enum
                if is_enum:
                    appending.append(" [[ enum ")
                else:
                    appending.append(" [[ class ")

                appending.append(cls_name_show)
                self.classes[cls_name].gen_readable_type_info(
                    appending, self, depth, short
                )
                appending.append(" ]] ")
            else:
                if has_non_external:
                    continue
                else:
                    cls_name_show = (
                        cls_name.split("/")[-1] if short else cls_name.replace("/", ".")
                    )
                    appending.append(f" [[ class {cls_name_show} {{ #EXTERNAL# }} ]] ")
                    break

    def gen_readable_method_info_params(
        self, method_name: str, depth: int = 5, short=True
    ):
        class_name, method_name = method_name.rsplit(".", 1)
        class_name = class_name.replace(".", "/")
        result = []
        self.classes[class_name].find_method(method_name).gen_readable_type_info_params(
            result, self, depth, short
        )
        return "".join(result)

    def gen_readable_method_info_ret(
        self, method_name: str, depth: int = 5, short=True
    ):
        class_name, method_name = method_name.rsplit(".", 1)
        class_name = class_name.replace(".", "/")
        result = []
        self.classes[class_name].find_method(method_name).gen_readable_type_info_ret(
            result, self, depth, short
        )
        return "".join(result)

    def get_argument_names(self, method_name: str):
        class_name, method_name = method_name.rsplit(".", 1)
        class_name = class_name.replace(".", "/")
        return self.classes[class_name].find_method(method_name).param_names

    def gen_readable_method_info_params_like_java(
        self, method_name: str, depth: int = 5, short=True
    ):
        class_name, method_name = method_name.rsplit(".", 1)
        class_name = class_name.replace(".", "/")
        result = []
        self.classes[class_name].find_method(
            method_name
        ).gen_readable_type_info_params_like_java(result, self, depth, short)
        return "".join(result)

    def gen_readable_method_info_ret_like_java(
        self, method_name: str, depth: int = 5, short=True
    ):
        class_name, method_name = method_name.rsplit(".", 1)
        class_name = class_name.replace(".", "/")
        result = []
        self.classes[class_name].find_method(
            method_name
        ).gen_readable_type_info_ret_like_java(result, self, depth, short)
        return "".join(result)


def load_java_info_from_file(file_path: str) -> JavaPackageInfo:
    package_info = JavaPackageInfo()

    current_class = None
    current_method = None

    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line = line.split(" ")

            if line[0] == "class":
                _, class_name, _, *modifiers = line
                is_enum = "enum" in modifiers
                is_interface = "interface" in modifiers
                is_abstract = "abstract" in modifiers
                current_class = JavaClassInfo(
                    class_name, is_enum, is_interface, is_abstract
                )
                package_info.classes[class_name] = current_class
            elif line[0] == "field":
                _, class_name, field_name, field_ty, field_signature, *modifiers = line
                if field_signature != "null":
                    field_ty = field_signature
                assert current_class.name == class_name
                is_static = "static" in modifiers
                field_info = JavaFieldInfo(field_name, field_ty)
                if is_static:
                    current_class.static_fields.append(field_info)
                else:
                    current_class.fields.append(field_info)
            elif line[0] == "super":
                _, class_name, super_name = line
                assert current_class.name == class_name
                current_class.super = super_name
            elif line[0] == "impl":
                _, class_name, impl_name = line
                assert current_class.name == class_name
                current_class.impls.append(impl_name)
            elif line[0] == "class_anno":
                pass
            elif line[0] == "class_anno_content":
                _, class_name, content = line
                content = json.loads(urllib.parse.unquote(content))
                assert current_class.name == class_name
                current_class.annos.append(content)
            elif line[0] == "method":
                _, class_name, method_name, method_ty, method_signature, *modifiers = (
                    line
                )
                assert current_class.name == class_name
                if method_signature != "null":
                    # FIXME currently, we do not consider method with generic types
                    if method_signature[0] == "(":
                        method_ty = method_signature
                current_method = JavaMethodInfo(method_name, method_ty)
                current_class.methods.append(current_method)
            elif line[0] == "param":
                _, class_name, method_name, param_name = line
                assert current_class.name == class_name
                assert current_method.name == method_name
                current_method.param_names.append(param_name)
            elif line[0] == "method_anno":
                pass
            elif line[0] == "method_anno_content":
                _, class_name, method_name, content = line
                assert current_class.name == class_name
                assert current_method.name == method_name
                content = json.loads(urllib.parse.unquote(content))
                current_method.annos.append(content)
            elif line[0] == "invoke":
                (
                    _,
                    class_name,
                    method_name,
                    invoke_class_name,
                    invoke_method_name,
                    invoke_signature,
                    *_,
                ) = line
                assert current_class.name == class_name
                assert current_method.name == method_name
                current_method.invoke.append(
                    (invoke_class_name, invoke_method_name, invoke_signature)
                )
            elif line[0] == "file":
                pass
            elif line[0] == "class_file":
                pass
            else:
                raise ValueError(f"Unknown line {line}")

    package_info.process()
    return package_info


class JavaInfoTest(unittest.TestCase):
    def test(self):
        from . import file_names

        package_info = load_java_info_from_file(file_names.train_ticket_services_file)
        test_cases = """train.service.TrainServiceImpl.retrieveByName
route.service.RouteServiceImpl.getRouteById
price.service.PriceServiceImpl.findByRouteIdAndTrainType
fdse.microservice.service.BasicServiceImpl.queryForTravel
other.service.OrderOtherServiceImpl.getSoldTickets
config.service.ConfigServiceImpl.query
seat.service.SeatServiceImpl.getLeftTicketOfInterval
other.service.OrderOtherServiceImpl.getSoldTickets
config.service.ConfigServiceImpl.query
seat.service.SeatServiceImpl.getLeftTicketOfInterval
travel2.service.TravelServiceImpl.getTripAllDetailInfo
fdse.microservice.service.StationServiceImpl.queryForId
fdse.microservice.service.StationServiceImpl.queryForId
train.service.TrainServiceImpl.retrieveByName
route.service.RouteServiceImpl.getRouteById
price.service.PriceServiceImpl.findByRouteIdAndTrainType
fdse.microservice.service.BasicServiceImpl.queryForTravel
other.service.OrderOtherServiceImpl.getSoldTickets
seat.service.SeatServiceImpl.distributeSeat
other.service.OrderOtherServiceImpl.create
assurance.service.AssuranceServiceImpl.create
foodsearch.service.FoodServiceImpl.createFoodOrder"""

        for test_case in test_cases.split("\n"):
            print(test_case)
            print(package_info.gen_readable_method_info_params(test_case))
            print(package_info.gen_readable_method_info_ret(test_case))
            print(package_info.gen_readable_method_info_params(test_case, short=False))
            print(package_info.gen_readable_method_info_ret(test_case, short=False))
            print(package_info.gen_readable_method_info_params_like_java(test_case))
            print(package_info.gen_readable_method_info_ret_like_java(test_case))
            print(
                package_info.gen_readable_method_info_params_like_java(
                    test_case, short=False
                )
            )
            print(
                package_info.gen_readable_method_info_ret_like_java(
                    test_case, short=False
                )
            )
            print()
