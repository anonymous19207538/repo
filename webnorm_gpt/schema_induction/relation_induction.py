from .db import DbTable, DbValue, ExpandedColumn

def fk1_to_fk2_mapping(fk1_col: ExpandedColumn, fk2_col: ExpandedColumn) -> dict[str, set[DbValue]]:
    fk1_to_fk2 = {}
    for fk1, fk2 in zip(fk1_col.values, fk2_col.values):
        if fk1 is not None and fk2 is not None:
            if fk1_col.schema.is_array():
                for fk1_val in fk1:
                    if fk1_val not in fk1_to_fk2:
                        fk1_to_fk2[fk1_val] = set()
                    if fk2_col.schema.is_array():
                        for fk2_val in fk2:
                            fk1_to_fk2[fk1_val].add(fk2_val)
                    else:
                        fk1_to_fk2[fk1_val].add(fk2)
            else:
                if fk1 not in fk1_to_fk2:
                    fk1_to_fk2[fk1] = set()
                if fk2_col.schema.is_array():
                    for fk2_val in fk2:
                        fk1_to_fk2[fk1].add(fk2_val)
                else:
                    fk1_to_fk2[fk1].add(fk2)
    return fk1_to_fk2

def infer_relation(right_column: ExpandedColumn, left_columns: list[ExpandedColumn]) -> dict[str, str]:
    relation_dict = {}
    for column_name, left_column in left_columns.items():
        if left_column.schema.is_basic():
            fk_to_left = fk1_to_fk2_mapping(right_column, left_column)
            left_to_fk = fk1_to_fk2_mapping(left_column, right_column)
            fk_has_multiple = any(len(v) > 1 for v in fk_to_left.values())
            left_has_multiple = any(len(v) > 1 for v in left_to_fk.values())
            if not fk_has_multiple and not left_has_multiple:
                relation_dict[column_name] = "one_to_one"
            elif fk_has_multiple and not left_has_multiple:
                relation_dict[column_name] = "one_to_many"
            elif not fk_has_multiple and left_has_multiple:
                relation_dict[column_name] = "many_to_one"
            else:
                relation_dict[column_name] = "many_to_many"
    return relation_dict

def infer_relations_in_table(table: DbTable) -> dict[str, dict[str, str]]:
    if len(table.join_info) == 0:
        relations, right_prefixes = [], []
    else:
        relations, right_prefixes = zip(*table.join_info)
    relation_dict = {}
    right_columns = {}
    left_columns = {}
    for column in table.expanded_columns:
        if any(column.name.startswith(prefix) for prefix in right_prefixes):
            right_columns[column.name] = column
        else:
            left_columns[column.name] = column

    for relation, right_prefix in table.join_info:
        if relation.ty == "foreign_key":
            foreign_key_name = f'{right_prefix}{relation.right_column}'
            foreign_key = right_columns[foreign_key_name]
            relation_dict[foreign_key_name] = {}
            column_relation_dict = infer_relation(foreign_key, left_columns)
            relation_dict[foreign_key_name] = column_relation_dict
        else:
            for right_column_name in right_columns:
                if right_column_name.startswith(right_prefix):
                    right_column = right_columns[right_column_name]
                    relation_dict[right_column_name] = {}
                    if right_column.schema.is_basic():
                        column_relation_dict = infer_relation(right_column, left_columns)
                        relation_dict[right_column_name] = column_relation_dict
                        
    return relation_dict