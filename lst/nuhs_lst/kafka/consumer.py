import logging
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
from nuhs_lst.utils.timer import Timer
import nuhs_lst.utils.log_utils as lu
import nuhs_lst.utils.exception_utils as eu

# Initialize logger
logger = logging.getLogger(__name__)


class KConsumer:
    """Kafka Consumer class for consuming messages from Kafka topics.

    This class handles subscribing to topics, consuming messages asynchronously or synchronously,
    performing health checks, and committing offsets. It also handles logging for each event.
    """

    def __init__(self, args, topics=None):
        """Initializes the Kafka Consumer with provided configuration.

        Args:
            args (dict): Configuration parameters including Kafka server details, consumer group, and other settings.
            topics (list, optional): List of topics to subscribe to. Defaults to None.
        """
        self.consumer = Consumer(
            {
                "bootstrap.servers": args["bootstrap.servers"],
                "group.id": args["group.id"],
                "auto.offset.reset": args["auto.offset.reset"],
                "enable.auto.commit": args["enable.auto.commit"],
                "sasl.mechanism": args["sasl.mechanism"],
                "security.protocol": args["security.protocol"],
                "ssl.ca.location": args["ssl.ca.location"],
                "sasl.username": args["sasl.username"],
                "sasl.password": args["sasl.password"],
                "client.id": f"{args['PROJECT_NAME']}_{socket.gethostname()}",
                "enable.ssl.certificate.verification": False,
            }
        )

        # Health parameters
        self._nuclear_timer = None
        self._time_limit = float(args["SELF_DESTRUCT_TIMER"])  # in seconds
        self._is_ready = False
        self.topics = topics
        self.consumer_async = args["CONSUMER_ASYNC"]

    def _start_detonation_countdown(self):
        """Starts the self-destruct countdown timer."""
        self._nuclear_timer = Timer()
        logger.info(
            f"Consumer connection lost. Self-Destruct timer activated. BOOM in {self._time_limit} secs!!!"
        )
        logger.info(lu.get_bomb_sign())

    def _has_exploded(self):
        """Checks if the self-destruct timer has expired.

        Returns:
            bool: True if the timer has expired, False otherwise.
        """
        elapsed_time = self._nuclear_timer.get_elapsed_seconds()
        return elapsed_time > self._time_limit

    def _is_alive(self):
        """Checks if the consumer is alive.

        Returns:
            bool: True if the consumer is alive (has assigned partitions), False otherwise.
        """
        return len(self._get_position()) != 0

    def _get_position(self):
        """Returns the consumer's partition assignment.

        Returns:
            list: A list of consumer's partition assignments.
        """
        return self.consumer.position(self.consumer.assignment())

    def _do_health_check(self):
        """Performs a health check on the consumer, triggering actions based on its state."""
        if not self._is_alive() and self._nuclear_timer is not None:
            if self._has_exploded():
                logger.info(lu.get_explosion_cloud())
                raise Exception("CONSUMER IS DEAD. BOOOM!!!!!")
        elif not self._is_alive():
            self._start_detonation_countdown()
        elif self._is_alive() and self._nuclear_timer is not None:
            logger.info(
                f"Consumer reconnected. {self._nuclear_timer.get_elapsed_time_string()} has passed. Turning off timer."
            )
            self._nuclear_timer = None
        else:
            self._nuclear_timer = None

        if not self._is_ready and self._is_alive():
            logger.info(lu.get_happy_sign())
            self._is_ready = True

    def consume(self, callback, topics=None):
        """Consumes messages from Kafka and processes them with a callback.

        Args:
            callback (function): A function that processes each consumed message.
            topics (list, optional): List of topics to subscribe to. Defaults to None.
        """
        topics = topics or self.topics
        self._consume(callback, topics)

    def _handle_msg(self, callback, msg):
        """Handles the consumed Kafka message, processes it, and logs the position.

        Args:
            callback (function): The function to process the message.
            msg (Message): The consumed Kafka message.
        """
        try:
            consumer_info = lu.format_kafka_msg(msg)
            logger.info(
                f">>>>>>>>>>>>>>>>> start offset: {consumer_info} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
            )
            logger.info("Start processing message")

            if msg is None:
                logger.error("MSG IS NULL!!!!!!!!!")
            else:
                callback(msg.value().decode("utf-8"), msg)

            logger.info(
                f"Consumer position (after processing message): {self._get_position()}"
            )
            logger.info(
                f"<<<<<<<<<<<<<<<< completed offset: {consumer_info} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n\n"
            )
        except Exception as e:
            logger.error(eu.get_exception_string(e))

    def _consume(self, callback, topics):
        """Subscribes to topics and starts consuming messages from Kafka.

        Args:
            callback (function): The function to process each consumed message.
            topics (list): List of Kafka topics to subscribe to.
        """
        try:
            logger.info(f"Topics: {topics}")
            self.consumer.subscribe(topics)
            logger.info(f"Consumer position: {self._get_position()}")

            while True:
                msg = self.consumer.poll(timeout=1.0)
                self._do_health_check()

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.error(
                            f"{msg.topic} [{msg.partition()}] reached end at offset {msg.offset()}\n"
                        )
                    else:
                        raise KafkaException(msg.error())
                else:
                    self._handle_msg(callback, msg)
                    self._commit()

        except Exception as e:
            logger.error(eu.get_exception_string(e))
        finally:
            self.consumer.close()

    def _commit(self):
        """Commits the consumer's offsets asynchronously or synchronously."""
        if self.consumer_async.lower() == "true":
            self.consumer.commit(asynchronous=True)
        elif self.consumer_async.lower() == "false":
            self.consumer.commit(asynchronous=False)
