"""
Script (27-02-2021) to fix version numbers in events table for events with migrations that may have the
wrong version numbers, due to an earlier bug in the migrations code.

This script will not put back the exact original version numbers on the events, but it will ensure compatibility.

It is hard to determine what the original event version should have been, but identifying the point at which an event
was definitely based on a newer version is easy.

This script is part of a migration; don't remove this.
"""

from gobcore.model.migrations import GOBMigrations
from typing import List, Optional

from gobupload.storage.handler import GOBStorageHandler


def _find_occurrence_of_column(catalog_name: str, collection_name: str, column: str, first_or_last: str,
                               storage: GOBStorageHandler):
    assert first_or_last in ('first', 'last')

    query = f"""
SELECT eventid FROM events
WHERE catalogue='{catalog_name}' AND entity='{collection_name}' AND (
    (action='ADD' AND (contents->'entity')::jsonb ? '{column}')
  OR
    (action='MODIFY' AND (contents->>'modifications')::jsonb @> '[{{\"key\": \"{column}\"}}]')
)
ORDER BY eventid {'ASC' if first_or_last == 'first' else 'DESC'}
LIMIT 1
"""
    with storage.get_session() as session:
        try:
            return next(session.execute(query))[0]
        except StopIteration:
            return None


def _find_last_occurrence_of_column(catalog_name: str, collection_name: str, column: str,
                                    storage: GOBStorageHandler) -> int:
    return _find_occurrence_of_column(catalog_name, collection_name, column, 'last', storage)


def _find_first_occurrence_of_column(catalog_name: str, collection_name: str, column: str,
                                     storage: GOBStorageHandler) -> int:
    return _find_occurrence_of_column(catalog_name, collection_name, column, 'first', storage)


def _get_change_eventid_for_conversion(catalog_name: str, collection_name: str, conversion: dict,
                                       storage: GOBStorageHandler):
    if conversion['action'] == 'add':
        # First eventid is the first occurrence of the newly added column
        eventid = _find_first_occurrence_of_column(catalog_name, collection_name, conversion['column'], storage)
    elif conversion['action'] == 'rename':
        eventid_last = _find_last_occurrence_of_column(catalog_name, collection_name, conversion['old_column'],
                                                       storage)
        eventid_first = _find_first_occurrence_of_column(catalog_name, collection_name, conversion['new_column'],
                                                         storage)
        eventid_last = eventid_last + 1 if eventid_last is not None else eventid_last
        eventid = min([e for e in [eventid_first, eventid_last] if e is not None]) if any(
            [eventid_first, eventid_last]) else None

    elif conversion['action'] == 'delete':
        eventid = _find_last_occurrence_of_column(catalog_name, collection_name, conversion['column'], storage)
        eventid = eventid + 1 if eventid is not None else eventid
    elif conversion['action'] == 'split_json':
        # Can't check anything here, as split_json does not add or remove columns, it just splits a value.
        # In practice a split_json conversion is used together with other (add/delete) conversions. Those
        # conversions can say something about the change event, a split_json can not.
        eventid = None
    else:
        raise NotImplementedError
    return eventid


def _get_change_eventid(catalog_name: str, collection_name: str, migration: dict, storage: GOBStorageHandler):
    """Returns the first eventid that's based on the new version, based on the occurrence of the migrated columns in
    the events

    Result can be None, when the columns in the migration never occur in the events

    :param migration:
    :return:
    """
    eventids = []
    for conversion in migration['conversions']:
        eventid = _get_change_eventid_for_conversion(catalog_name, collection_name, conversion, storage)
        if eventid is not None:
            eventids.append(eventid)

    return min(eventids) if eventids else None


def _update_version_numbers(catalog_name: str, collection_name: str, change_eventids: List[tuple],
                            storage: GOBStorageHandler):
    """

    :param catalog_name:
    :param collection_name:
    :param change_eventids: List of two-tuples of ordered version strings with the first eventid that should have this
    version
    :return:
    """

    def update_events(min_eventid: int, max_eventid: Optional[int], version: str):
        where_eventids = f"eventid >= {min_eventid}" + (f" AND eventid < {max_eventid}" if max_eventid else "")

        query = f"UPDATE events SET version='{version}' " \
                f"WHERE catalogue='{catalog_name}' AND entity='{collection_name}' " \
                f"AND {where_eventids}"
        storage.execute(query)

    print(f"{catalog_name} {collection_name}: Update version numbers in events table")

    for i, (version, start_eventid) in enumerate(change_eventids[:-1]):
        end_eventid = change_eventids[i + 1][1]

        print(f"{catalog_name} {collection_name}: Version {version} ranges from eventid >= {start_eventid} and "
              f"eventid < {end_eventid}")
        update_events(start_eventid, end_eventid, version)

    # Do last
    rest = change_eventids[-1]
    version = rest[0]
    start_eventid = rest[1]

    print(f"{catalog_name} {collection_name}: Version {version} from eventid >= {start_eventid} to end")
    update_events(start_eventid, None, version)


def correct_version_numbers():
    storage = GOBStorageHandler()
    for catalog_name, collection in GOBMigrations()._migrations.items():
        for collection_name, versions in collection.items():

            print(f"{catalog_name} {collection_name}: Determine version boundaries in events")

            current_version = '0.1'
            change_eventids = [(current_version, 0)]
            migration = versions[current_version]

            while migration:
                change_eventid = _get_change_eventid(catalog_name, collection_name, migration, storage)
                target_version = migration['target_version']

                if change_eventid is not None:
                    change_eventids.append((target_version, change_eventid))

                migration = versions.get(target_version)

            _update_version_numbers(catalog_name, collection_name, change_eventids, storage)


if __name__ == "__main__":
    correct_version_numbers()
