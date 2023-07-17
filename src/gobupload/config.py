import os

FULL_UPLOAD = "full"

DEBUG = True if os.getenv("DEBUG") else False

GOB_DB = {
    "drivername": "postgresql",
    "username": os.getenv("DATABASE_USER", "gob"),
    "database": os.getenv("DATABASE_NAME", "gob"),
    "password": os.getenv("DATABASE_PASSWORD", "insecure"),
    "host": os.getenv("DATABASE_HOST_OVERRIDE", "localhost"),
    "port": os.getenv("DATABASE_PORT_OVERRIDE", 5406),
}
