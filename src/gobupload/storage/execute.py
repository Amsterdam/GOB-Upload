from gobupload.storage.handler import GOBStorageHandler


def _execute_multiple(queries, stream=False, max_row_buffer=1000):
    handler = GOBStorageHandler()

    with handler.get_session() as session:

        if stream:
            connection = session.connection(execution_options={
                'stream_results': True,
                'max_row_buffer': max_row_buffer
            })
            execute_on = connection
        else:
            execute_on = session
        # Commit all queries as a whole on exit with
        for query in queries:
            result = execute_on.execute(query)

    return result   # Return result of last execution


def _execute(query, stream=False, max_row_buffer=1000):
    return _execute_multiple([query], stream, max_row_buffer)
