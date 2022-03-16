import sys

from gobupload.relate.update import Relater


def run():
    assert len(sys.argv) >= 4, "Missing arguments: relate_query.py " \
                               "gebieden wijken ligt_in_stadsdeel [ initial/conflicts ]"

    catalog = sys.argv[1]
    collection = sys.argv[2]
    attribute = sys.argv[3]
    option = sys.argv[4] if len(sys.argv) >= 5 else None
    initial = option == 'initial'
    check_conflicts = option == 'conflicts'

    relater = Relater(catalog, collection, attribute)

    start_src_event, max_src_event, start_dst_event, max_dst_event = relater._get_changed_ranges()

    kwargs = {
        'start_src_event': 0 if initial else start_src_event,
        'max_src_event': max_src_event,
        'start_dst_event': 0 if initial else start_dst_event,
        'max_dst_event': max_dst_event,
        'only_src_side': initial
    }
    src_entities_query, dst_entities_query = next(relater._get_chunks(**kwargs))

    print(
        relater.get_query(
            src_entities=src_entities_query,
            dst_entities=dst_entities_query,
            max_src_event=max_src_event,
            max_dst_event=max_dst_event,
            is_conflicts_query=check_conflicts
        )
    )

    if relater.src_has_states or relater.dst_has_states:
        print("")
        print("NOTE: Temporary tables are not created. Create the temporary tables with the following commands.")

        if relater.src_has_states:
            print(f"python -m gobupload.dev_utils.relate_interval_table {catalog} {collection} "
                  f"{relater.src_intv_tmp_table.name}")

        if relater.dst_has_states and relater.src_intv_tmp_table.name != relater.dst_intv_tmp_table.name:
            print(f"python -m gobupload.dev_utils.relate_interval_table {relater.dst_catalog_name} "
                  f"{relater.dst_collection_name} {relater.dst_intv_tmp_table.name}")


if __name__ == "__main__":
    """
    python -m gobupload.dev_utils.relate_query gebieden wijken ligt_in_stadsdeel [ initial/conflicts ]

    Prints the update_table relate query. Prints the full query by default, but prints the query for the initial import
    when the fourth parameter is set to 'initial'. (The initial query omits the unnecessary second part of the UNION).
    Print the conflicts query when the fourth parameter is set to 'conflicts'.
    """
    run()
