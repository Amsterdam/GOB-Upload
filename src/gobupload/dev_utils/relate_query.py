import sys

from gobupload.relate.table.update_table import RelationTableRelater


def run():
    assert len(sys.argv) >= 4, "Missing arguments: relate_query.py " \
                               "gebieden wijken ligt_in_stadsdeel [ initial/conflicts ]"

    catalog = sys.argv[1]
    collection = sys.argv[2]
    attribute = sys.argv[3]
    initial = False if len(sys.argv) == 4 else sys.argv[4].lower() == 'initial'
    check_conflicts = False if len(sys.argv) == 4 else sys.argv[4].lower() == 'conflicts'

    relater = RelationTableRelater(catalog, collection, attribute)

    print(relater.get_conflicts_query() if check_conflicts else relater.get_query(initial))


if __name__ == "__main__":
    """
    python -m gobupload.dev_utils.relate_query gebieden wijken ligt_in_stadsdeel [ inital/conflicts ]

    Prints the update_table relate query. Prints the full query by default, but prints the query for the initial import
    when the fourth parameter is set to 'initial'. (The initial query omits the unnecessary second part of the UNION).
    Print the conflicts query when the fourth parameter is set to 'conflicts'.
    """
    run()
