import json
import os
import re

import javalang




def get_Impl_files_path(proj_folder):
    java_files = []
    for root, dirs, files in os.walk(proj_folder):
        if "src/main/java" in root:
            if "service" in root:
                for file in files:
                    if file.endswith("Impl.java"):
                        java_files.append(os.path.join(root, file))
            if "controller" in root:
                for file in files:
                        java_files.append(os.path.join(root, file))
    java_files.sort()
    return java_files


# get all entity imports in the Impl files
def get_entity_imports(file, parsed_classes):
    with open(file, "r") as f:
        code = f.read()
    tree = javalang.parse.parse(code)
    imports = []
    for path, node in tree.filter(javalang.tree.Import):

        if "entity." in node.path or (node.wildcard and "entity" in node.path):
            path = node.path
            if node.wildcard:
                for key in parsed_classes.keys():
                    if path in key:
                        imports.append(key)
            else:
                imports.append(path)
    return imports

def get_nested_type_string(ref_type):
    if ref_type is None:
        return "void"

    result = ref_type.name

    # Check if the type has generic arguments
    if hasattr(ref_type, 'arguments') and ref_type.arguments:
        inner_types = [get_nested_type_string(arg.type) for arg in ref_type.arguments]
        result += f"<{', '.join(inner_types)}>"

    return result

def get_method_related_entity(Impl_file, entity_full_paths, method_related_entities, class_name):
    with open(Impl_file, "r") as f:
        code = f.read()

    # Parse the code into an abstract syntax tree (AST)
    tree = javalang.parse.parse(code)
    methods = []
    entity_classes = {path.split(".")[-1]: path for path in entity_full_paths}

    type_pattern = (
        r"type=ReferenceType\(arguments=.*?, dimensions=.*?, name=.*?, sub_type=.*?\)"
    )
    name_match = r"name=([\w]+)"

    # Filter and process method declarations
    for path, node in tree.filter(javalang.tree.MethodDeclaration):
        method_name = node.name

        parameters = [
            (param.name, param.type.name if param.type else "UnknownType")
            for param in node.parameters
        ]

        # Initialize an empty set to track entity usage
        entities = set()

        # Add entity information based on parameters
        for param in node.parameters:
            if param.type and param.type.name in entity_classes:
                entities.add(entity_classes[param.type.name])

        # Traverse the method body for variable declarations and method invocations that use entities
        if node.body:
            for method_node in node.body:

                # print(method_node)
                matches = re.findall(type_pattern, str(method_node))
                for match in matches:
                    for name in re.findall(name_match, match):
                        if name in entity_classes:
                            entities.add(entity_classes[name])
        # Get the return type of the method
        return_type = get_nested_type_string(node.return_type)

        pattern = r'([A-Za-z0-9_]+(?:<[^>]*>)?)'
        return_matches = re.findall(pattern, return_type) 

        for match in return_matches:
            if match.strip() in entity_classes:
                entities.add(entity_classes[match])

        return_type = node.return_type.name if node.return_type else "void"
        method_related_entities[class_name + "." + method_name] = {
            "input_parameters": parameters,
            "return_type": return_type,
            "entity": list(entities) if entities else None,

        }
        

    return method_related_entities


def main():
    cur_path = os.path.dirname(os.path.abspath(__file__))
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--p", type=str, help="Project name")
    args = arg_parser.parse_args()

    if args.p == "nicefish":
        project_folder = os.path.join(
            cur_path, "../../../target-projects/WebNormPlus-tgt-nicefish-spring-cloud"
        )
        entity_file =  "../../../generated/entity_nicefish.json"
        output_file = "../../../generated/method_related_entity_nicefish.json"
    elif args.p == "train_ticket":
        project_folder = os.path.join(
            cur_path, "../../../target-projects/WebNormPlus-tgt-train-ticket"
        )
        entity_file = "../../../generated/entity_train_ticket.json"
        output_file = "../../../generated/method_related_entity_train_ticket.json"
    else:
        print("Invalid project name")
        return

    Impl_files = get_Impl_files_path(project_folder)

    with open(os.path.join(cur_path, entity_file), "r") as f:
        parsed_classes = json.load(f)

    file_imports = {}
    for file in Impl_files:
        file_imports[file] = get_entity_imports(file, parsed_classes)

    method_related_entities = {}
    for file, imports in file_imports.items():
        rel_path = os.path.relpath(file, project_folder)
        class_name = rel_path.split('src/main/java/')[1].replace("/", ".").replace(".java", "")
        method_related_entities = get_method_related_entity(file, imports, method_related_entities, class_name)

    with open(os.path.join(cur_path, output_file), "w") as f:
        json.dump(method_related_entities, f, indent=4)


if __name__ == "__main__":
    main()
