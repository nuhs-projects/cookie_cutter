import pandas as pd
import numpy as np
import nuhs_lst.utils.file_utils as fu
import os

import logging

logger = logging.getLogger(__name__)


def is_empty(x):
    if x == np.nan or x == "None" or x is None or x == "":
        return True
    else:
        return False


def print_args(args):
    for k, v in args.items():
        # if k!= 'DB_PASSWORD':
        logger.debug(f"{k}: {v}")
    # logger.debug(getLibrarySign())


def setup_args():
    args = fu.read_yaml("./nuhs_lst/config/config.yaml")
    for k in args:
        args[k] = os.getenv(k, default=args[k])
    return args
