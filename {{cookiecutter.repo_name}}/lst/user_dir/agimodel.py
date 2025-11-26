import random
import logging

logger = logging.getLogger(__name__)


class AGILos:

    def __init__(self):
        # load model weights here
        pass

    def predict(self, note, age, gender):

        logger.info(f"AGE: {age}")
        logger.info(f"GENDER: {gender}")

        note = note.replace("<br>", "\n")
        words = note.split(" ")
        attention_weights = [random.uniform(0, 1) for w in words]
        return random.randint(1, 30), attention_weights, words
