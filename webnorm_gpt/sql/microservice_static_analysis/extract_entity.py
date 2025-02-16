import json
import os

import javalang


# loop through all folders and get all java files path under 'entity' folder
def get_entity_files_path(proj_folder):
    java_files = []
    for root, dirs, files in os.walk(proj_folder):
        if "entity" in root:
            for file in files:
                if file.endswith(".java"):
                    java_files.append(os.path.join(root, file))
    java_files.sort()
    return java_files


def parse_entity_file(file):
    with open(file, "r") as f:
        code = f.read()

    tree = javalang.parse.parse(code)

    classes = {}
    # enums = {}

    # Parse class declarations
    for path, node in tree.filter(javalang.tree.ClassDeclaration):
        class_name = node.name
        fields = []
        for field in node.fields:
            for declarator in field.declarators:
                field_name = declarator.name
                field_type = field.type.name
                fields.append((field_name, field_type))
        classes[class_name] = fields

    # Parse enum declarations
    for path, node in tree.filter(javalang.tree.EnumDeclaration):
        enum_name = node.name
        # enum_constants = []

        # # Extract enum constants
        # for constant in node.body.constants:
        #     enum_constants.append(constant.name)

        fields = []

        # Extract enum fields (like 'code' and 'name')
        for decl in node.body.declarations:
            if isinstance(decl, javalang.tree.FieldDeclaration):
                field_type = decl.type.name
                for declarator in decl.declarators:
                    field_name = declarator.name
                    fields.append((field_name, field_type))

        classes[enum_name] = fields

    return classes


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
        output_file = "../../../generated/entity_nicefish.json"
    elif args.p == "train_ticket":
        project_folder = os.path.join(
            cur_path, "../../../target-projects/WebNormPlus-tgt-train-ticket"
        )
        output_file = "../../../generated/entity_train_ticket.json"
    else:
        print("Invalid project name")
        return
    
    entity_files = get_entity_files_path(project_folder)
    parsed_classes = {}

    for file in entity_files:
        rel_path = os.path.relpath(file, project_folder)
        file_name = rel_path.split("java")[1][1:-1].replace("/", ".")
        # file_name = os.path.basename(file)
        # entity_name = file_name.split('.')[0]
        parsed_classes[file_name] = parse_entity_file(file)

    with open(os.path.join(cur_path, output_file), "w") as f:
        json.dump(parsed_classes, f, indent=4)


if __name__ == "__main__":
    main()
