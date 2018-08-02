"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import time

from gobuploadservice.config import MESSAGE_BROKER, get_workflow_queue
from gobuploadservice.message_broker.async_message_broker import AsyncConnection

from gobuploadservice.compare import compare
from gobuploadservice.update import full_update


WORKFLOW = {
    'fullimport.request': {'handler': compare, 'report_back': 'fullupdate.proposal'},
    'fullupdate.request': {'handler': full_update, 'report_back': 'fullupdate.proposal'}
}


def on_message(connection, queue, key, msg):
    """Called on every message receipt

    :param connection: the connection with the message broker
    :param queue: the message broker queue
    :param key: the identification of the message (e.g. fullimport.proposal)
    :param msg: the contents of the message
    :return:
    """

    print(f"{key} accepted from {queue['name']}, start compare")

    handle = WORKFLOW[key]['handler']
    report_back = WORKFLOW[key]['report_back']

    try:
        result_msg = handle(msg)
    except RuntimeError:
        return False

    connection.publish(get_workflow_queue(report_back), report_back, result_msg)
    return True


# Start a connection with the message broker
with AsyncConnection(MESSAGE_BROKER) as connection:
    # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
    for key in WORKFLOW.keys():
        connection.subscribe(get_workflow_queue(key), on_message)

    # Repeat forever
    print("Update component started")
    while True:
        time.sleep(60)
        # Report some statistics or whatever is useful
        print(".")
