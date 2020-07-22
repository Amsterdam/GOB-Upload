import sys

from gobupload.relate.update import StartValiditiesTable


def run():
    assert len(sys.argv) == 4, "Missing arguments: relate_interval_table.py " \
                               "gebieden wijken table_name"

    catalog = sys.argv[1]
    collection = sys.argv[2]
    table_name = sys.argv[3]

    table = StartValiditiesTable.from_catalog_collection(catalog, collection, table_name)
    table.create()
    print(f"Table {table.name} created")


if __name__ == "__main__":
    """
    python -m gobupload.dev_utils.relate_interval_table gebieden wijken [table_name]

    Builds the interval table as used by relate: the table with id, volgnummer, begin_geldigheid for each object

    """
    run()
