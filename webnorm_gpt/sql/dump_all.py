from ..schema_induction.db import DbDump


def infer_not_null(dump: DbDump) -> list[tuple[str, str]]:
    results = []

    for table in dump.tables:
        for column in table.columns:
            if None not in column.values:
                results.append((table.name, column.name))

    return results


def infer_unique(dump: DbDump) -> list[tuple[str, str]]:
    results = []

    for table in dump.tables:
        for column in table.columns:
            if len(set(column.values)) == len(column.values):
                results.append((table.name, column.name))

    return results


def infer_foreign_key(dump: DbDump) -> list[tuple[str, str, str, str]]:
    results = []

    all_columns = []
    for table in dump.tables:
        for column in table.columns:
            all_columns.append((table.name, column.name))

    not_null_filter = lambda x: x is not None

    for i, (table_i, column_i) in enumerate(all_columns):
        data_i = dump.find_table(table_i).find_column(column_i).values
        data_i_set = set(filter(not_null_filter, data_i))
        if len(data_i_set) == 0:
            continue
        for j, (table_j, column_j) in enumerate(all_columns):
            data_j = dump.find_table(table_j).find_column(column_j).values
            data_j_set = set(filter(not_null_filter, data_j))
            if len(data_j_set) == 0:
                continue
            if i != j:
                if data_i_set.issubset(data_j_set):
                    # print(f"{table_i}.{column_i} -> {table_j}.{column_j}")
                    results.append((table_i, column_i, table_j, column_j))

    return results

    # not_null = infer_not_null(db_dump)
    # pprint.pprint(not_null)

    # unique = infer_unique(db_dump)
    # pprint.pprint(unique)

    # foreign_key = infer_foreign_key(db_dump)
    # pprint.pprint(foreign_key)

    # infer constraints: not null, unique, foreign key

    # one-to-one
    # one-to-many
    # many-to-many
    # seperated-one-to-many
