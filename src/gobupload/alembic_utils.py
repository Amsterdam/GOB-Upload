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
