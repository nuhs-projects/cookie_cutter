import socket
import json
import logging
from confluent_kafka import Producer
import nuhs_lst.utils.log_utils as lu

# Initialize logger
logger = logging.getLogger(__name__)


class KProducer:
    """Kafka Producer class for managing Kafka message production.

    This class encapsulates the logic for configuring and using a Kafka producer to send messages to a specified topic.
    It supports both synchronous and asynchronous message production.
    """

    def __init__(self, args, topic=None):
        """Initializes the Kafka Producer with provided configuration.

        Args:
            args (dict): Configuration parameters including Kafka server details and other settings.
            topic (str, optional): The default topic for producing messages. Defaults to None.
        """
        self.producer = Producer(
            {
                "bootstrap.servers": args["bootstrap.servers"],
                "sasl.mechanism": args["sasl.mechanism"],
                "security.protocol": args["security.protocol"],
                "ssl.ca.location": args["ssl.ca.location"],
                "sasl.username": args["sasl.username"],
                "sasl.password": args["sasl.password"],
                "client.id": f"{args['PROJECT_NAME']}_{socket.gethostname()}",
                "message.send.max.retries": args["message.send.max.retries"],
                "retry.backoff.ms": args["retry.backoff.ms"],
                "enable.ssl.certificate.verification": False,
            }
        )

        self._time_limit = float(args["SELF_DESTRUCT_TIMER"])
        self.topic = topic
        self.producer_async = args.get("PRODUCER_ASYNC", "TRUE")

    def poll(self, timeout):
        """Polls the producer for events.

        Args:
            timeout (int): Timeout in seconds for waiting for events.

        Returns:
            int: Number of events processed.
        """
        return self.producer.poll(timeout)

    def check_status(self):
        """Checks the status of the producer by listing available topics."""
        logger.info("Checking producer status...")
        self.producer.list_topics(timeout=self._time_limit)

    def produce(self, data, topic=None, check_status=True, report_callback=None):
        """Produces a message to Kafka.

        Args:
            data (dict or str): The message data to be sent to Kafka.
            topic (str, optional): The topic to send the message to. Defaults to the instance's `topic`.
            check_status (bool, optional): Whether to check the producer status before sending. Defaults to True.
            report_callback (function, optional): Callback function for reporting delivery status. Defaults to None.
        """
        topic = topic or self.topic
        self._produce(data, topic, check_status, report_callback)

    def _produce(self, data, topic, check_status=True, report_callback=None):
        """Internal method to produce a message to Kafka.

        Args:
            data (dict or str): The message data to be sent to Kafka.
            topic (str): The topic to send the message to.
            check_status (bool, optional): Whether to check the producer status before sending. Defaults to True.
            report_callback (function, optional): Callback function for reporting delivery status. Defaults to None.
        """
        if check_status:
            self.check_status()

        if isinstance(data, dict):
            data = json.dumps(data).encode("utf-8")

        logger.info(f"Producing message to topic: {topic}")

        # Use default delivery report callback if none provided
        report_callback = report_callback or self.delivery_report
        self.producer.produce(topic, value=data, on_delivery=report_callback)

        if self.producer_async.lower() == "false":
            self.producer.flush(1)
            logger.info(f"Messages still in queue: {self.producer.flush()}")
        else:
            self.producer.poll(1)
            logger.info(f"Event(s) processed (callbacks served by producer)")

    def delivery_report(self, err, msg):
        """Reports the success or failure of a message delivery.

        Args:
            err (KafkaError): The error that occurred on None if success.
            msg (Message): The Kafka message that was produced or failed.
        """
        logger.info("***************************************************************")
        self._delivery_report(err, msg)
        logger.info(
            "***************************************************************\n\n"
        )

    def _delivery_report(self, err, msg):
        """Logs the details of the message delivery status.

        Args:
            err (KafkaError): The error that occurred on None if success.
            msg (Message): The Kafka message that was produced or failed.
        """
        logger.info("Delivery report")
        if err is not None:
            logger.error(
                f"DELIVERY REPORT: Delivery failed for User record {msg.key()}: {err}"
            )
        else:
            logger.info(f"DELIVERY REPORT: {lu.format_kafka_msg(msg)}")
