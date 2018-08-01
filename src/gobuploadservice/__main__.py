"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import time

from gobuploadservice.config import MESSAGE_BROKER, QUEUES, WORKFLOW_QUEUE
from gobuploadservice.message_broker.async_message_broker import AsyncConnection

from gobuploadservice.compare import compare
from gobuploadservice.update import full_update


def publish_request_result(connection, key, result_msg):
    """Publish the result of a request

    :param connection: The message broker connection
    :param key: The key to identify the type of message
    :param result: The message
    :return:
    """
    post = {
        "exchange": "gob.workflow",
        "name": "gob.workflow.proposal",
        "key": "*.proposal"
    }
    connection.publish(post, key, result_msg)


def on_message(connection, queue, key, msg):
    """Called on every message receipt

    :param connection: the connection with the message broker
    :param queue: the message broker queue
    :param key: the identification of the message (e.g. fullimport.proposal)
    :param msg: the contents of the message
    :return:
    """

    if queue["name"] == WORKFLOW_QUEUE:
        # Request has been received
        if key == "fullimport.request":
            print("Fullimport.request accepted, start compare")
            result_msg = compare(msg)
            publish_request_result(connection, "fullupdate.proposal", result_msg)
        elif key == "fullupdate.request":
            print("Fullupdate.request accepted, start update")
            result_msg = full_update(msg)
            publish_request_result(connection, "updatefinished.proposal", result_msg)
        else:
            print("Unknown request received", key)
            return False  # ignore message, leave for someone else
    else:
        # This should never happen, the subscription routing_key should filter the messages
        print("Unknown message type received", queue["name"], key)
        return False  # Do not acknowledge the message

    return True  # Acknowledge message when it has been fully handled


# Start a connection with the message broker
with AsyncConnection(MESSAGE_BROKER) as connection:

    # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
    connection.subscribe(QUEUES, on_message)

    # Repeat forever
    print("Update component started")
    while True:
        time.sleep(60)
        # Report some statistics or whatever is useful
        print(".")
