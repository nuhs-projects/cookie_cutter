import time
import logging

logger = logging.getLogger(__name__)


class Timer:
    def __init__(self):
        self.start = time.time()

    def print_elapsed_time(self):
        logger.info(self.get_elapsed_time_string())

    def get_elapsed_time_string(self):
        end = time.time()
        hours, rem = divmod(end - self.start, 3600)
        minutes, seconds = divmod(rem, 60)
        return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)

    def get_elapsed_seconds(self):
        end = time.time()
        esecs = end - self.start
        return esecs
