from dataclasses import dataclass


def get_query_split_json_column(table_name: str, json_col_name: str, mapping: dict[str, str],
                                json_attr_types: dict[str, str]):
    """
    :param table_name: Table we are operating on
    :param json_col_name: The JSON column name
    :param mapping: A mapping from attributes in the JSON field to column name

    For example:
    table_name: nap_peilmerken
    json_col_name: status
    mapping: {code: status_code, omschrijving: status_omschrijving}
    json_attr_types: {code: int, omschrijving: varchar}

    This results in the columns status_code and status_omschrijving to be filled with the code and omschrijving
    values present in the json column 'status'.
    """

    set_expr = ", ".join([f"{column_name}=({json_col_name}->>'{json_attr}')::{json_attr_types[json_attr]}"
                          for json_attr, column_name in mapping.items()])
    return f"UPDATE {table_name} SET {set_expr}"


def get_query_merge_columns_to_jsonb_column(table_name: str, json_col_name: str, mapping: dict[str, str]):
    """
    The inverse action of the json_column_to_separate_columns function above
    """
    jsonb_build_object_args = ", ".join([
        f"'{json_attr}', \"{column_name}\"" for json_attr, column_name in mapping.items()
    ])

    return f"UPDATE {table_name} SET {json_col_name} = jsonb_build_object({jsonb_build_object_args})"


@dataclass
class RenamedRelation:
    table_name: str
    old_column: str
    new_column: str
    old_relation_table: str
    new_relation_table: str


def upgrade_relations(op, relations: list[RenamedRelation]):
    for relation in relations:
        _rename_relation(
            op,
            relation.table_name,
            relation.old_column,
            relation.new_column,
            relation.old_relation_table,
            relation.new_relation_table
        )


def downgrade_relations(op, relations: list[RenamedRelation]):
    for relation in relations:
        _rename_relation(
            op,
            relation.table_name,
            relation.new_column,
            relation.old_column,
            relation.new_relation_table,
            relation.old_relation_table
        )


def _rename_relation(op, table_name: str, old_column: str, new_column: str,
                     old_relation_table: str, new_relation_table: str):
    op.rename_table(old_relation_table, new_relation_table)
    op.alter_column(table_name, old_column, new_column_name=new_column)

    old_relation_name = old_relation_table.replace("rel_", "", 1)
    new_relation_name = new_relation_table.replace("rel_", "", 1)
    rename_events_query = f"UPDATE events SET entity = '{new_relation_name}' " \
                          f"WHERE catalogue='rel' AND entity = '{old_relation_name}'"

    # Create partitions for events if they don't exist yet
    op.execute(
        "CREATE TABLE IF NOT EXISTS events.rel PARTITION OF events FOR VALUES IN ('rel') PARTITION BY LIST (entity)")
    op.execute(
        f"CREATE TABLE IF NOT EXISTS events.rel_{new_relation_name} PARTITION OF events.rel "
        f"FOR VALUES IN ('{new_relation_name}') PARTITION BY LIST(source)")
    op.execute(
        f"CREATE TABLE IF NOT EXISTS events.rel_{new_relation_name}_gob PARTITION OF events.rel_{new_relation_name} "
        f"FOR VALUES IN ('GOB')")
    op.execute(rename_events_query)
