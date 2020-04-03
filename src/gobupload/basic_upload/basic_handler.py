# import datetime

from gobcore.events.import_message import ImportMessage
from gobcore.events.import_events import CONFIRM

from gobupload.compare.basic_compare import get_events
from gobupload.update.basic_update import store_events
from gobupload.apply.basic_apply import apply_unhandled_events, apply_events, apply_confirms

from gobupload.storage.handler import GOBStorageHandler


# test_msg = {
#     'header': {
#         'catalogue': 'kwaliteit',
#         'entity': 'bevindingen',
#         'source': 'AMSBI',
#         'application': 'Grondslag',
#         'timestamp': datetime.datetime.utcnow().isoformat(),
#         'version': '0.1'
#     },
#     'contents': [{
#         '_source_id':
#             'AMSBI.Grondslag.Value_height_6_15.IMPORT.AMSBI.Grondslag.nap.peilmerken.hoogte_tov_nap.44380009',
#         'attribuut': 'hoogte_tov_nap',
#         'betwijfelde_waarde': None,
#         'code': 'Value_height_6_15',
#         'identificatie': '44380005',
#         'meldingnummer': 'Value_height_6_15.IMPORT.AMSBI.Grondslag.nap.peilmerken.hoogte_tov_nap.44380009',
#         'objectklasse': 'peilmerken',
#         'onderbouwing': None,
#         'proces': 'IMPORT',
#         'registratie': 'nap',
#         'volgnummer': None,
#         'voorgestelde_waarde': None
#     }],
#     'summary': {
#         'num_records': 1
#     }
# }


def handle_msg(msg):
    # Parse the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    # Get entities to process
    entities = message.contents or []
    if not entities:
        # Nothing to do
        return

    # Initialize storage for the metadata of the message
    storage = GOBStorageHandler(metadata)

    # Apply any unhandled events and get highest entity event
    entity_max_eventid = apply_unhandled_events(storage)

    # Compare
    events = get_events(msg, storage, entities)

    # First handle confirm events
    confirms = [event for event in events if event['event'] == CONFIRM.name]
    if confirms:
        # Apply confirms with date = msg timestamp
        apply_confirms(storage, confirms, metadata.timestamp)

    # Then handle any other events
    other_events = [event for event in events if event not in confirms]
    if other_events:
        # Update (store) events
        store_events(storage, other_events)
        # Apply events
        apply_events(storage, entity_max_eventid)
