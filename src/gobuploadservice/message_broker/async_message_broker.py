# flake8: noqa
# todo: check if this file can be simplified and/or shared in multiple GOB components
import json
import threading

import pika


def progress(*args):
    """Utility function to facilitate debugging

    :param args: args that will be printed
    :return: None
    """

    print("Progress", threading.get_ident(), *args)


class AsyncConnection(object):
    """This is an asynchronous RabbitMQ connection.

    It handles unexpected conditions when interacting with RabbitMQ

    Threading and locks are used to provide for a synchronous connection setup and
    to allow both publishing and subscribing in parallel.

    The connection allows for an unlimited number of subscriptions.

    Extensive use of closures is made to handle the asynchronous communication with RabbitMQ

    No automatic reconnection with RabbitMQ is implemented.

    """

    def __init__(self, address):
        """Create a new AsyncConnection

        :param address: The RabbitMQ address
        """

        # The address of the RabbitMQ Message broker
        self._address = address

        # The Connection and Channel objects
        self._connection = None
        self._channel = None

        # The RabbitMQ eventloop thread
        self._eventloop = None

        # Optional method, called on connection established
        self._on_connect_callback = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def _on_open_connection(self, connection):
        """Called on successful connection to RabbitMQ

        A channel will be created for the communication with RabbitMQ

        :param connection: The connection that has been establishe
        :return: None
        """

        # Register the connection
        self._connection = connection

        def on_close_channel(channel, code, text):
            """Called when a channel is closed

            :param channel: The channel that is closed
            :param code: The code for the channel closure
            :param text: The test for the channel closure
            :return: None
            """

            progress("Channel closed:", code, text)
            self.disconnect()

        def on_open_channel(channel):
            """Called when a channel has been successfully established

            :param channel: The Channel object
            :return: None
            """

            # If a callback has been defined for connection success, call this function
            if self._on_connect_callback:
                self._on_connect_callback()
                # Call it only once
                self._on_connect_callback = None

            # Release the lock set in the connect() function so that this function can return the result
            self._lock.release()

        # Create a Channel, on_open_channel is called on success
        self._channel = connection.channel(on_open_callback=on_open_channel)

        # Register a function to handle the closure of the channel
        self._channel.add_on_close_callback(callback=on_close_channel)

    def connect(self, on_connect_callback=None):
        """Connect to RabbitMQ.

        :param on_connect_callback: This function will be called when a connection has been established
        :return bool: True when the connection has been established, False otherwise
        """

        def on_error_connection(connection, text):
            """This function is called on a connection error

            The lock that has been set on connection creation will be released.
            The connect function will be released and notice that no channel has been created.
            It will therefore return False to inform the caller that no connection could be established

            The disconnect method will be called to clean up the connection properties and stop the eventloop.

            :param connection: The connection object
            :param text: The error text
            :return: None
            """

            progress("Connection error:", text)
            self._lock.release()
            self.disconnect()

        def on_close_connection(connection, code, text):
            """Called when a connection is closed

            :param connection: The RabbitMQ connection object
            :param code: The error code
            :param text: The error text
            :return: None
            """

            progress("Connection closed:", code, text)
            self._connection = None

        def eventloop():
            """The RabbitMQ eventloop.

            An ioloop is started to listen for messages for the active subscriptions.

            :return: None
            """

            try:
                self._connection.ioloop.start()
            except Exception as e:
                progress("Eventloop exception:", e)
            progress("Eventloop ended")

        # A callback function can be specified that will be called when a connection is established
        self._on_connect_callback = on_connect_callback

        # Obtain a lock. The lock will be release on connection establishment or on failure
        self._lock = threading.Lock()
        self._lock.acquire()

        # Create a connection object
        self._connection = pika.SelectConnection(
            parameters=pika.ConnectionParameters(self._address),
            on_open_callback=self._on_open_connection,
            on_open_error_callback=on_error_connection,
            on_close_callback=on_close_connection)

        # Start the RabbitMQ eventloop
        self._eventloop = threading.Thread(target=eventloop)
        self._eventloop.start()

        # Wait for the lock to be released to be able to report success or failure
        self._lock.acquire()

        # Check whether a channel has been created, which means the connection has been established
        return self._channel is not None

    def publish(self, queue, key, msg):
        """Publish a message on a queue

        The message will be converted to json before publishing it on the queue

        :param queue: The queue object
        :param msg: The message
        :return: None
        """

        # Check whether a connection has been established
        if self._channel is None:
            raise Exception("Connection with message broker not available")

        # Convert the message to json
        json_msg = json.dumps(msg)

        # Publish the message as a persistent message on the queue
        self._channel.basic_publish(
            exchange=queue["exchange"],
            routing_key=key,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=json_msg
        )

    def subscribe(self, queues, message_handler):
        """Subscribe to the given queues

        :param queues: The queues to subscribe on
        :return: None
        """

        def on_message(queue):
            """This function is called for every message that is received

            The specified handler will be called to let the application handle the message.
            Default behaviour is to acknowledge the message after it has been successfully handled.
            If the handler returns False the message will not be acknowledged and stay on the queue

            :param queue: The queue that is consumed by this on_message
            :return: The handle message function
            """

            def handle_message(channel, basic_deliver, properties, body):
                """Handle the incoming message

                The message body is a json object which will be parsed on receipt

                Call the handler and acknowledge the message after it has been successfully handled

                :param channel: The channel that represents the connection with RabbitMQ
                :param basic_deliver: The deliver properties, e.g. is redelivery, routing_key, ...
                :param properties: general message properties
                :param body: The message body (json dump)
                :return: None
                """

                # Try to parse body as json message, else pass it as it is received
                msg = body
                try:
                    msg = json.loads(body)
                except json.decoder.JSONDecodeError:
                    pass

                if message_handler(self, queue, basic_deliver.routing_key, msg) is not False:
                    # Default is to acknowledge message
                    channel.basic_ack(basic_deliver.delivery_tag)

            return handle_message

        def on_queue_bind(queue):
            """Calles on successfully bind to the given queue

            A consumer will be created to consume the messages from the queue

            :param queue: The queue that is consumed by this on_message
            :param queue: The name of the queue
            :return: A method that links the handler to a consumer
            """
            return lambda frame: self._channel.basic_consume(
                consumer_callback=on_message(queue),
                queue=queue["name"])

        def on_queue_declare(queue):
            return lambda q: self._channel.queue_bind(
                    callback=on_queue_bind(queue),
                    exchange=queue["exchange"],
                    queue=queue["name"],
                    routing_key=queue["key"]
                )

        # Subscribe to each queue in the list
        for queue in queues:
            self._channel.queue_declare(
                callback=on_queue_declare(queue),
                queue=queue["name"],
                durable=True
            )

    def disconnect(self):
        """Disconnect from RabbitMQ

        Close any open channels
        Close the connection
        Stop any running eventloop

        :return: None
        """

        progress("Disconnect")

        # Close any open channel
        if self._channel is not None and self._channel.is_open:
            progress("Close channel")
            # Cancel consumers
            for consumer_tag in self._channel.consumer_tags:
                self._channel.basic_cancel(
                    callback=lambda frame: None,
                    consumer_tag=consumer_tag)
            self._channel.close()
        self._channel = None

        # Close any open connection
        if self._connection is not None:
            progress("Close connection")
            self._connection.close()
            # Wait for eventloop to finish on closing of the connection
            if self._eventloop is not None and threading.get_ident() != self._eventloop.ident:
                self._eventloop.join()
                self._eventloop = None
        self._connection = None
