import sys

from gobupload.relate.update import Relater
from gobupload.storage.handler import GOBStorageHandler


def run():
    assert len(sys.argv) >= 4, "Missing arguments: relate_query.py " \
                               "gebieden wijken ligt_in_stadsdeel [ initial/conflicts ]"

    class MetaData:
        catalogue = sys.argv[1]
        entity = sys.argv[2]
        attribute = sys.argv[3]

    option = sys.argv[4] if len(sys.argv) >= 5 else None
    check_conflicts = option == 'conflicts'

    with GOBStorageHandler(gob_metadata=MetaData).get_session() as session:
        relater = Relater(session, MetaData.catalogue, MetaData.entity, MetaData.attribute)

        max_src_event = relater._get_max_src_event()
        max_dst_event = relater._get_max_dst_event()

        src_entity_query, dst_entities_query = next(
            relater._get_chunks(0, max_src_event, 0, max_dst_event, only_src_side=True)
        )

        print(
            relater.get_query(
                src_entity_query,
                dst_entities_query,
                max_src_event=max_src_event,
                max_dst_event=max_dst_event,
                is_conflicts_query=check_conflicts
            )
        )

    if relater.src_has_states or relater.dst_has_states:
        print("")
        print("NOTE: Temporary tables are not created. Create the temporary tables with the following commands.")

        if relater.src_has_states:
            print(f"python -m gobupload.dev_utils.relate_interval_table {MetaData.catalogue} {MetaData.entity} "
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
