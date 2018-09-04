import os

GOB_DB = {
    'drivername': 'postgres',
    'username': 'gob',
    'password': 'insecure',
    'host': os.getenv("DB_HOST", "localhost"),
    'port': 5406
}
