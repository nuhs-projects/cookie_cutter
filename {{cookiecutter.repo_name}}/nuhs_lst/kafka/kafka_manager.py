import socket
import sys
import json
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import nuhs_lst.utils.log_utils as lu
import nuhs_lst.utils.exception_utils as eu
from nuhs_lst.kafka.consumer import KConsumer
from nuhs_lst.kafka.producer import KProducer
import logging

# Initialize logger
logger = logging.getLogger(__name__)


class KafkaManager:
    """Manages Kafka consumers and producers for processing messages and predictions.

    This class consumes messages from a Kafka topic, processes them using a model,
    enriches the data, and sends the predictions to a producer topic.
    """

    def __init__(self, args, model, dbm):
        """Initializes KafkaManager with necessary arguments, model, and database manager.

        Args:
            args (dict): Configuration arguments including topics for consumer and producer.
            model (LSTModel): The model used for message processing and prediction.
            dbm (DBManager): The database manager for enriching the data (if applicable).
        """
        self.consumer = KConsumer(args)
        self.producer = KProducer(args)

        self.consumer_topics = [args["CONSUMER_TOPIC"]]
        self.model = model
        self.producer_topics = args["PRODUCER_TOPIC"]
        self.dbm = dbm

    def start(self):
        """Starts the Kafka consumer to begin message consumption and processing."""
        self.consumer.consume(self.process_msg, topics=self.consumer_topics)

    def process_msg(self, msg, raw_msg):
        """Processes a single Kafka message by predicting and enriching the data.

        Args:
            msg (dict): The Kafka message to process.
            raw_msg (Message): The raw Kafka message to extract metadata.

        Logs the process at each stage and handles errors gracefully.
        """
        try:
            logger.info("Start processing message")
            data = self.model.process_msg(msg)

            if data is not None:
                logger.info("Enriching data :)")
                data = self.model.enrich(self.dbm, data)

                logger.info("Data enrichment successful. Start model prediction :D")
                result = self.model.predict(
                    data, metadata={"consumerInfo": lu.format_kafka_msg(raw_msg)}
                )

                if result is not None:
                    logger.info(
                        f"Prediction successful. Start sending to {self.producer_topics} topic :P"
                    )
                    self.producer.produce(
                        result,
                        topic=self.producer_topics,
                        report_callback=self.custom_report,
                    )
            else:
                logger.info(
                    "Processed Message returned None. Skipping enrichment and prediction."
                )

        except Exception as e:
            logger.error(f"Error processing message: {eu.get_exception_string(e)}")

    def custom_report(self, err, msg):
        """Callback function to handle the delivery report from Kafka producer.

        Logs the delivery status of the message, including metadata and any errors.

        Args:
            err (KafkaError or None): Error returned from Kafka producer (if any).
            msg (Message): The Kafka message that was produced.
        """
        logger.info(lu.get_asterisk_long_delimiter())
        self.producer._delivery_report(err, msg)

        try:
            if err is not None:
                pstr = "DELIVERY REPORT: Consumer info of failed message -> "
            else:
                pstr = "DELIVERY REPORT: Consumer info of delivered message -> "

            data = json.loads(msg.value().decode("utf-8"))
            metadata = json.loads(data["result_prediction"]["metadata"])
            pstr += metadata["consumerInfo"]

            if "uid" in metadata:
                pstr += f' and uid -> {metadata["uid"]}'

            logger.info(pstr)
        except Exception as e:
            logger.error(
                f"Error processing delivery report: {eu.get_exception_string(e)}"
            )
            logger.error("Unable to get consumer info from outflow message")
        logger.info(lu.get_asterisk_long_delimiter())
