import os

MESSAGE_BROKER = os.environ["MESSAGE_BROKER_ADDRESS"]

WORKFLOW_QUEUE = "gob.workflow.request"

QUEUES = [
    {
        "exchange": "gob.workflow",
        "name": WORKFLOW_QUEUE,
        "key": "*.request"
    }
]

GOB_DB = {'drivername': 'postgres',
          'username': 'gob',
          'password': 'insecure',
          'host': os.getenv("DB_HOST", "localhost"),
          'port': 5406}
