import os

MESSAGE_BROKER = os.environ["MESSAGE_BROKER_ADDRESS"]

WORKFLOW_QUEUE = "gob.workflow.request"
WORKFLOW_EXCHANGE = "gob.workflow"


def get_workflow_queue(key):
    return {
        "exchange": WORKFLOW_EXCHANGE,
        "name": WORKFLOW_QUEUE,
        "key": key
    }


GOB_DB = {
    'drivername': 'postgres',
    'username': 'gob',
    'password': 'insecure',
    'host': os.getenv("DB_HOST", "localhost"),
    'port': 5406
}
