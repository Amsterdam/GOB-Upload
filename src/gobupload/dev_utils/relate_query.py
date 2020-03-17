import sys

from gobupload.relate.table.update_table import RelationTableRelater


def run():
    assert len(sys.argv) >= 4, "Missing arguments: relate_query.py gebieden wijken ligt_in_stadsdeel [ true ]"

    catalog = sys.argv[1]
    collection = sys.argv[2]
    attribute = sys.argv[3]
    initial = False if len(sys.argv) == 4 else sys.argv[4].lower() == 'true'

    relater = RelationTableRelater(catalog, collection, attribute)

    print(relater.get_query(initial))


if __name__ == "__main__":
    """
    python -m gobupload.dev_utils.relate_query gebieden wijken ligt_in_stadsdeel [ true ]

    Prints the update_table relate query. Prints the full query by default, but prints the query for the initial import
    when the fourth parameter is set to 'true'. (The initial query omits the unnecessary second part of the UNION).
    """
    run()
