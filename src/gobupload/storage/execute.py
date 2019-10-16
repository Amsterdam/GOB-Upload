from gobupload.storage.handler import GOBStorageHandler


def _execute_multiple(queries):
    handler = GOBStorageHandler()

    with handler.get_session() as session:
        connection = session.connection(execution_options={'stream_results': True})
        # Commit all queries as a whole on exit with
        for query in queries:
            result = connection.execute(query)

    return result   # Return result of last execution


def _execute(query):
    return _execute_multiple([query])
