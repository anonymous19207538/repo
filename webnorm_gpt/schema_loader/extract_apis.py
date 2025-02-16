import argparse

from .. import file_names, javainfo
from ..file_types.proj_desc_file import APIDesc, ProjDescFile

CONTROLLER_ANNOTATIONS = set(
    [
        "Lorg/springframework/stereotype/Controller;",
        "Lorg/springframework/web/bind/annotation/RestController;",
    ]
)

MAPPING_ANNOTATIONS = {
    "Lorg/springframework/web/bind/annotation/DeleteMapping;": "delete",
    "Lorg/springframework/web/bind/annotation/GetMapping;": "get",
    "Lorg/springframework/web/bind/annotation/PatchMapping;": "patch",
    "Lorg/springframework/web/bind/annotation/PostMapping;": "post",
    "Lorg/springframework/web/bind/annotation/PutMapping;": "put",
    "Lorg/springframework/web/bind/annotation/RequestMapping;": "none",
}

SERVICE_ANNOTATIONS = set(["Lorg/springframework/stereotype/Service;"])


def get_all_controllers(
    package_info: javainfo.JavaPackageInfo,
) -> list[tuple[javainfo.JavaClassInfo, str, list]]:
    results = []
    for cls in package_info.classes.values():
        has_controller = False

        for anno in cls.annos:
            if anno["descriptor"] in CONTROLLER_ANNOTATIONS:
                has_controller = True
                # root_path = anno["fields"].get("value", "/")
                # print(anno)
                break

        if not has_controller:
            continue

        root_paths = []
        req_methods = []
        for anno in cls.annos:
            if anno["descriptor"] in MAPPING_ANNOTATIONS:
                # print(anno)
                # root_paths.extend(anno["fields"].get("value", []))
                for k, v in anno["fields"].items():
                    if k == "value":
                        root_paths.extend(v)
                    if k == "path":
                        root_paths.extend(v)
                req_method = MAPPING_ANNOTATIONS[anno["descriptor"]]
                if req_method != "none":
                    req_methods.append(req_method)

        req_methods = list(set(req_methods))

        # print(root_paths)
        if len(root_paths) == 0:
            root_path = "/"
        elif len(root_paths) == 1:
            root_path = root_paths[0]
        else:
            print("Multiple root paths found for", cls.name, root_paths)
            root_path = root_paths[0]

        if root_path[0] != "/":
            root_path = "/" + root_path

        # print(cls.name, root_path)
        results.append((cls, root_path, req_methods))

    return results


def get_mapping_methods(
    cls: javainfo.JavaClassInfo,
    root_path: str,
    req_methods: list[str],
) -> list[tuple[javainfo.JavaClassInfo, javainfo.JavaMethodInfo, list[str]]]:
    results = []
    for method in cls.methods:
        mappings = []
        req_methods_cur = []
        for anno in method.annos:
            if anno["descriptor"] in MAPPING_ANNOTATIONS:
                mappings.append(anno)
                r = MAPPING_ANNOTATIONS[anno["descriptor"]]
                if len(req_methods) == 0 or r in req_methods:
                    req_methods_cur.append(r)

        if len(mappings) == 0:
            continue

        paths = []
        for m in mappings:
            for k, v in m["fields"].items():
                if k == "value":
                    paths.extend(v)
                if k == "path":
                    paths.extend(v)

        paths_ext = []
        for path in paths:
            if path.startswith("/"):
                paths_ext.append(root_path + path)
            else:
                paths_ext.append(root_path + "/" + path)

        if root_path != "/" and len(paths_ext) == 0:
            paths_ext.append(root_path)

        paths = paths_ext

        if len(paths) == 0:
            print("No path found for", cls.name, method.name)
            print(root_path)

        req_methods_cur = list(set(req_methods_cur))
        if len(req_methods_cur) > 1:
            print(
                "Multiple req methods found for", cls.name, method.name, req_methods_cur
            )
        if len(req_methods_cur) == 0:
            print("No req methods found for", cls.name, method.name)
            req_methods_cur = "none"
        else:
            req_methods_cur = req_methods_cur[0]

        results.append((cls, method, paths, req_methods_cur))

    return results


def is_service_class(cls: javainfo.JavaClassInfo) -> bool:
    for anno in cls.annos:
        if anno["descriptor"] in SERVICE_ANNOTATIONS:
            return True
    return False


def find_all_invocations(
    package_info: javainfo.JavaPackageInfo, method: javainfo.JavaMethodInfo
) -> list[str]:
    all_invoked_methods = []
    for invoked_cls, invoked_name, invoked_signature in method.invoke:
        if invoked_cls not in package_info.classes:
            continue
        invoked_cls_info = package_info.classes[invoked_cls]

        invoked_method = None
        for target_method in invoked_cls_info.methods:
            if (
                target_method.name
                == invoked_name
                # and target_method.ty == invoked_signature
            ):
                invoked_method = target_method
                break
        if invoked_method is None:
            continue

        if not invoked_cls_info.is_interface and is_service_class(invoked_cls_info):
            all_invoked_methods.append(
                invoked_cls_info.name.replace("/", ".") + "." + invoked_name
            )

        for clz2 in package_info.classes.values():
            if clz2.is_interface:
                continue
            if not is_service_class(clz2):
                continue
            if invoked_cls in clz2.impls:
                for target_method2 in clz2.methods:
                    if (
                        target_method2.name
                        == invoked_name
                        # and target_method2.ty == invoked_signature
                    ):
                        all_invoked_methods.append(
                            clz2.name.replace("/", ".") + "." + invoked_name
                        )

    return all_invoked_methods


def train_ticket_append_extra_api_descs(
    api_descs: list[APIDesc], package_info: javainfo.JavaPackageInfo
):
    api_desc = APIDesc()
    api_desc.name = "rebook.service.RebookServiceImpl.updateOrder"
    api_desc.def_req = package_info.gen_readable_method_info_params_like_java(
        api_desc.name
    )
    api_desc.def_resp = package_info.gen_readable_method_info_ret_like_java(
        api_desc.name
    )
    api_desc.src = None
    api_desc.extra = {
        "controller_path": "rebook.service.RebookServiceImpl.updateOrder",
        "url_path": "/api/v1/rebookservice/updateorder",
        "req_method": "post",
    }
    api_descs.append(api_desc)

    api_desc = APIDesc()
    api_desc.name = "rebook.service.RebookServiceImpl.payDifferentMoney"
    api_desc.def_req = package_info.gen_readable_method_info_params_like_java(
        api_desc.name
    )
    api_desc.def_resp = package_info.gen_readable_method_info_ret_like_java(
        api_desc.name
    )
    api_desc.src = None

    api_desc.extra = {
        "controller_path": "rebook.service.RebookServiceImpl.payDifferentMoney",
        "url_path": "/api/v1/rebookservice/paydifferentmoney",
        "req_method": "post",
    }
    api_descs.append(api_desc)


def main(input_path: str, output_path: str, proj_name: str, method_type: str):
    package_info = javainfo.load_java_info_from_file(input_path)
    controllers = get_all_controllers(package_info)
    mappings = []
    for controller, root_path, req_methods in controllers:
        mappings.extend(get_mapping_methods(controller, root_path, req_methods))

    api_descs = []
    skip_api_descs = list()

    for cls_name, method, urls, req_method in mappings:
        cls_name = cls_name.name.replace("/", ".")
        method_name = method.name

        if len(urls) > 1:
            print(
                "Multiple urls found for",
                cls_name,
                method_name,
                urls,
            )
            continue

        if method_type == "service":
            invoked_methods = find_all_invocations(package_info, method)
            invoked_methods = list(set(invoked_methods))

            if len(urls) == 0 or len(invoked_methods) == 0:
                skip_api_descs.append((cls_name, method_name, invoked_methods, urls))
                continue
            if len(invoked_methods) > 1:
                print(
                    "Multiple invoked methods found for",
                    cls_name,
                    method_name,
                    invoked_methods,
                    urls,
                )
                continue

            target_invoked = invoked_methods[0]
        elif method_type == "controller":
            target_invoked = cls_name + "." + method_name
        else:
            raise ValueError(f"Unknown method type: {method_type}")

        def_req = package_info.gen_readable_method_info_params_like_java(target_invoked)
        def_resp = package_info.gen_readable_method_info_ret_like_java(target_invoked)
        argument_names = package_info.get_argument_names(target_invoked)

        api_desc = APIDesc()
        api_desc.name = target_invoked
        api_desc.def_req = def_req
        api_desc.def_resp = def_resp
        api_desc.src = None
        api_desc.argument_names = argument_names
        api_desc.extra = {
            "controller_path": f"{cls_name}.{method_name}",
            "url_path": urls[0],
            "req_method": req_method,
        }
        api_descs.append(api_desc)

    if proj_name == "train-ticket":
        train_ticket_append_extra_api_descs(api_descs, package_info)

    proj_desc = ProjDescFile()
    proj_desc.name = proj_name
    proj_desc.apis = api_descs
    proj_desc.save_to_path(output_path)

    for cls_name, method_name, invoked_methods, urls in skip_api_descs:
        print("Skipped", cls_name, method_name, invoked_methods, urls)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True, type=str)
    parser.add_argument("-o", "--output", required=True, type=str)
    parser.add_argument("-n", "--name", required=True, type=str)
    parser.add_argument(
        "-m", "--method_type", required=True, choices=["controller", "service"]
    )
    args = parser.parse_args()
    main(args.input, args.output, args.name, args.method_type)


if __name__ == "__main__":
    parse_args()
