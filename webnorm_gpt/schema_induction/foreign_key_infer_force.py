from collections import defaultdict

import tqdm

from ..gpt_invoker import GPTInvoker
from .db import DbDump, DbTable, ExpandedColumn
from .foreign_key_prompt import (
    EXTRACT_RELATED_TABLE_SYSTEM,
    FILTER_COLUMNS_SYSTEM,
    FILTER_COLUMNS_USER,
    FOREIGN_KEY_PROMPT_FORMAT_CANDIDATE,
    FOREIGN_KEY_PROMPT_SYSTEM,
    FOREIGN_KEY_PROMPT_USER,
    EXTRACT_RELATED_Table_USER,
)
from .schema import JsonSchemaTypes


def is_non_null(x):
    return x is not None


def set_filter_non_null(x):
    return set(filter(is_non_null, x))


LOG_META_DATA_COLUMNS = {
    "log_data.api",
    "log_data.api_name",
    "log_data.has_error",
    "log_data.response_time",
    "log_data.throwable",
    "log_data.seq",
    "log_data.time_parsed",
    "log_data.time_response_parsed",
}

LOG_DATA_EXPANDED_META_DATA = {"#length", "#exists"}


def filter_columns(
    gpt_invoker: GPTInvoker, db: DbDump
) -> list[tuple[DbTable, list[ExpandedColumn], list[ExpandedColumn]]]:
    result = []
    for table in tqdm.tqdm(
        db.tables, total=len(db.tables), desc="Extracting Possible Foreign Key"
    ):
        table_columns = []
        for column in table.expanded_columns:
            if (
                column.schema.ty in [JsonSchemaTypes.Str, JsonSchemaTypes.Int]
                and "log_data.headers" not in column.name
                and column.name not in LOG_META_DATA_COLUMNS
                and not any(
                    expanded_meta in column.name
                    for expanded_meta in LOG_DATA_EXPANDED_META_DATA
                )
            ):
                table_columns.append(column)
        column_str = "\n".join(
            [f"{column.name} {column.schema.basic_name()}" for column in table_columns]
        )
        # print(column_str)
        filter_columns_inputs = [
            {
                "role": "system",
                "content": FILTER_COLUMNS_SYSTEM,
            },
            {
                "role": "user",
                "content": FILTER_COLUMNS_USER.format(
                    columns=column_str, table_name=table.name
                ),
            },
        ]
        filter_columns_response = gpt_invoker.generate(filter_columns_inputs)
        filter_columns_response = gpt_invoker.extract_json(filter_columns_response)

        foreign_keys_column = [
            column
            for column in table_columns
            if column.name in filter_columns_response["foreign_keys"]
        ]
        if table.name.startswith("log::"):
            primary_keys_column = []
        else:
            primary_keys_column = [
                column
                for column in table_columns
                if column.name in filter_columns_response["primary_keys"]
            ]

        result.append((table, foreign_keys_column, primary_keys_column))
    return result


def extract_relavant_tables(
    gpt_invoker: GPTInvoker,
    table_column_pairs: list[tuple[DbTable, list[ExpandedColumn]]],
    related_entity_dict: dict,
) -> dict[str, list[str]]:
    related_tables_dict = {}
    db_list = [
        table for table, _ in table_column_pairs if table.name.startswith("db::")
    ]
    db_list_name = [table.name.split("::")[1] for table in db_list]
    for table, columns in tqdm.tqdm(
        table_column_pairs,
        total=len(table_column_pairs),
        desc="Extracting Related Tables",
    ):
        related_entity_list = []
        related_tables = []
        if len(columns) > 0:
            if table.name.startswith("log::"):
                api_name = table.name.split("::")[1]
                related_entity_list = related_entity_dict.get(api_name, {}).get("entity", [])
            column_str = "\n".join(
                [f"{column.name} {column.schema.basic_name()}" for column in columns]
            )
            extract_related_table_inputs = [
                {
                    "role": "system",
                    "content": EXTRACT_RELATED_TABLE_SYSTEM,
                },
                {
                    "role": "user",
                    "content": EXTRACT_RELATED_Table_USER.format(
                        db_list=str(db_list_name),
                        columns=column_str,
                        entity_list=str(related_entity_list),
                    ),
                },
            ]
            extract_related_table_response = gpt_invoker.generate(
                extract_related_table_inputs
            )
            extract_related_table_response = gpt_invoker.extract_json(
                extract_related_table_response
            )
            related_tables = [
                table.name
                for table in db_list
                if table.name.split("::")[1] in extract_related_table_response["tables"]
            ]
        related_tables_dict[table.name] = related_tables
    return related_tables_dict


def infer_foreign_key(
    table_column_triplets: list[
        tuple[DbTable, list[ExpandedColumn], list[ExpandedColumn]]
    ],
    related_tables_dict: dict[str, list[str]],
) -> list[tuple[str, str, str, str]]:
    foreign_keys = []
    # table_column_triplet: [table, foreign_keys_columns, primary_keys_columns]
    for from_table, from_columns, _ in table_column_triplets:
        for from_column in from_columns:
            # if not from_column.schema.is_primary():
            #     continue
            from_values = set_filter_non_null(from_column.values)
            from_values_length = len(from_values)
            if from_values_length == 0:
                continue
            for to_table, _, to_columns in table_column_triplets:
                if to_table.name not in related_tables_dict[from_table.name]:
                    continue
                if from_table.name == to_table.name:
                    continue
                if to_table.name.startswith("log::"):
                    continue

                for to_column in to_columns:
                    # if not to_column.schema.is_primary():
                    #     continue

                    to_values = set_filter_non_null(to_column.values)
                    if len(to_values) == 0:
                        continue
                    intersection_length = len(from_values.intersection(to_values))
                    if intersection_length / from_values_length > 0.2:
                        foreign_keys.append(
                            (
                                from_table.name,
                                from_column.name,
                                to_table.name,
                                to_column.name,
                            )
                        )

    return foreign_keys


def gpt_foreign_key_filter(
    foreign_keys: list[tuple[str, str, str, str]], gpt_invoker: GPTInvoker
):
    src_target_map = defaultdict(list)
    for from_table, from_column, to_table, to_column in foreign_keys:
        src_target_map[(from_table, from_column)].append((to_table, to_column))

    foreign_key_results = []

    for (from_table, from_column), to_list in tqdm.tqdm(
        src_target_map.items(), total=len(src_target_map), desc="GPT Foreign Key Filter"
    ):
        from_table_prefix, from_table_name = from_table.split("::", 1)

        candidates = []
        for to_table, to_column in to_list:
            to_table_prefix, to_table_name = to_table.split("::", 1)
            candidate = FOREIGN_KEY_PROMPT_FORMAT_CANDIDATE.format(
                table_name=to_table_name, field_name=to_column
            )
            candidates.append(candidate)
        candidates = "\n".join(candidates)

        prompt_system = FOREIGN_KEY_PROMPT_SYSTEM
        prompt_user = FOREIGN_KEY_PROMPT_USER.format(
            target_table_name=from_table_name,
            target_field_name=from_column,
            candidate_table_field_pairs=candidates,
        )

        prompts = [
            {"role": "system", "content": prompt_system},
            {"role": "user", "content": prompt_user},
        ]
        response = gpt_invoker.generate(prompts)
        response = gpt_invoker.extract_json(response)

        foreign_keys_gpt = response["foreign_keys"]
        appended_table = set()
        for foreign_key in foreign_keys_gpt:
            if foreign_key["table"] in appended_table:
                continue
            foreign_key_results.append(
                (
                    from_table,
                    from_column,
                    "db::" + foreign_key["table"],
                    foreign_key["field"],
                )
            )

    return foreign_key_results
