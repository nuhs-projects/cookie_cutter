import sys
import os
import logging
import json
from nuhs_lst.web import start_web_service
import nuhs_lst.utils.log_utils as lu
import nuhs_lst.utils.gen_utils as gu
from nuhs_lst.db_manager import DBManager
from nuhs_lst.kafka.producer import KProducer
from nuhs_lst.kafka.kafka_manager import KafkaManager

# Append custom user directory to sys path
sys.path.append(os.path.abspath("./user_dir"))
from user_dir.lst_model import LSTModel

# Initialize logger
logger = logging.getLogger(__name__)


def strike():
    """Main function to handle arguments, set up logging, and trigger actions.

    Depending on the provided arguments, this function will either start the
    live service or test Kafka message production.
    """
    # Set up arguments and logging
    args = gu.setup_args()
    lu.setup_logger(logLevel=args["LOG_LEVEL"])
    gu.print_args(args)
    logger.info(lu.get_library_sign())

    # Execute corresponding actions based on the input arguments
    if len(sys.argv) > 1:
        action = sys.argv[1]
        if action == "live":
            live(args)
        elif action == "testProduce":
            test_produce(args)

    logger.info("Program completed!")


def live(args):
    """Start the live service by loading the model, initializing the DBManager,
    and starting the web service and Kafka consumer.

    Args:
        args (dict): Configuration and arguments for the service.
    """
    # Load the LST model
    model = LSTModel(args["PROJECT_NAME"])

    # Initialize DBManager if not disabled
    dbm = DBManager(args) if not args["NO_DBM"] else None
    logger.info(dbm)

    # Start the web service
    start_web_service()

    logger.info("Starting Kafka consumer...")

    # Load and start the Kafka consumer
    km = KafkaManager(args, model, dbm)
    km.start()


def test_produce(args):
    """Test Kafka message production by reading a sample file and producing
    messages to the specified Kafka topic.

    Args:
        args (dict): Configuration and arguments for the Kafka producer.
    """
    # Print arguments and project name
    print(json.dumps(args, sort_keys=True, indent=4))
    print(args["PROJECT_NAME"])

    # Initialize Kafka producer
    producer = KProducer(args)

    # Read sample data from file
    with open("./user_dir/testing/test_files/live/sample_hl7.json") as fp:
        data = json.load(fp)

    # Produce message to Kafka topic
    producer.produce(data, topic="nuhs.eai.hl7.mdm")
