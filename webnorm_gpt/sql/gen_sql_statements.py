import json
import os
import pickle
import ast
import zstandard
import copy
import traceback
from tqdm import tqdm
from ..file_types.log_file import LogFile, LogItem
from ..file_types.proj_desc_file import ProjDescFile
from ..gpt_invoker import GPTInvoker
from .prompt import (
    EXTRACT_RELATED_ENTITY_SYSTEM,
    EXTRACT_RELATED_ENTITY_USER,
    EXTRACT_FOREIGN_KEY_ENTITY_SYSTEM,
    EXTRACT_FOREIGN_KEY_ENTITY_USER,
    USER_CONTEXT_SYSTEM,
    USER_CONTEXT_USER,
    GENERATE_SQL_SYSTEM,
    GENERATE_SQL_USER,
)


def generate_sql_statements(
    logs: LogFile, proj_desc: ProjDescFile, gpt_invoker: GPTInvoker, entity_dict
) -> dict[str, dict[str, str]]:

    sql_statements = {}
    for api in tqdm(proj_desc.apis):
        api_name = api.to_json()["name"]
        try:
            function_header = api.to_json()["def_req"].split("class")[0]
            # extract entity from function header
            extract_entity_inputs = [
                {
                    "role": "system",
                    "content": EXTRACT_RELATED_ENTITY_SYSTEM.format(
                        schema_list=list(entity_dict.keys())
                    ),
                },
                {
                    "role": "user",
                    "content": EXTRACT_RELATED_ENTITY_USER.format(
                        function_header=function_header,
                    ),
                },
            ]

            entity_response = gpt_invoker.generate(extract_entity_inputs)
            related_entity_list = ast.literal_eval(entity_response)
            final_entity_list = set(copy.deepcopy(related_entity_list))

            if len(related_entity_list) > 0:
                # extract foreign key entity for each entity
                for entity in related_entity_list:
                    class_ = {entity: entity_dict[entity]}
                    extract_foreign_key_entity_inputs = [
                        {
                            "role": "system",
                            "content": EXTRACT_FOREIGN_KEY_ENTITY_SYSTEM.format(
                                schema_list=list(entity_dict.keys())
                            ),
                        },
                        {
                            "role": "user",
                            "content": EXTRACT_FOREIGN_KEY_ENTITY_USER.format(
                                class_=class_
                            ),
                        },
                    ]
                    entity_response = gpt_invoker.generate(
                        extract_foreign_key_entity_inputs
                    )
                    foreign_key_entity_list = ast.literal_eval(entity_response)
                    final_entity_list.update(foreign_key_entity_list)
                final_entites = {
                    entity: entity_dict[entity] for entity in final_entity_list
                }
                # decide whether to extract current login user information
                user_context_inputs = [
                    {"role": "system", "content": USER_CONTEXT_SYSTEM},
                    {
                        "role": "user",
                        "content": USER_CONTEXT_USER.format(
                            function_header=function_header,
                            entities=final_entites,
                        ),
                    },
                ]
                user_context_response = gpt_invoker.generate(user_context_inputs)
                if user_context_response == "Yes":
                    user_context = "userId, username"
                else:
                    user_context = "Not available"

                # generate sql query
                generate_sql_inputs = [
                    {"role": "system", "content": GENERATE_SQL_SYSTEM},
                    {
                        "role": "user",
                        "content": GENERATE_SQL_USER.format(
                            function_header=function_header,
                            entities=final_entites,
                            user_context=user_context,
                        ),
                    },
                ]
                sql_response = gpt_invoker.generate(generate_sql_inputs)
                sql_dict = ast.literal_eval(sql_response)
                sql_statements[api_name] = sql_dict
        except Exception as e:
            traceback.print_exc()
            sql_statements[api_name] = {}

    # Example result
    return sql_statements


def main():
    with zstandard.open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../traffic-new-collect/night-instru-logs.pkl.zst",
        ),
        "rb",
    ) as f:
        logs: LogFile = pickle.load(f)

    proj_desc = ProjDescFile()
    proj_desc.load_from_file_path(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../generated/train-ticket-projdesc.json.zst",
        )
    )

    gpt_invoker = GPTInvoker(
        model="gpt-4o",
        dump_gpt_log=True,
    )

    with zstandard.open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../generated/train-ticket-database-schema.json.zst",
        ),
        "r",
    ) as f:
        parsed_entity_dict = json.load(f)
    # write parsed_entity_dict to a file

    sqls = generate_sql_statements(logs, proj_desc, gpt_invoker, parsed_entity_dict)

    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../../generated/sqls.json"
        ),
        "w",
    ) as f:
        f.write(json.dumps(sqls, indent=4))


if __name__ == "__main__":
    main()
