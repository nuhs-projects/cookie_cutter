import logging


def setup_logger(logLevel="INFO"):
    console_format = (
        BColors.OKBLUE
        + "[%(levelname)s]"
        + BColors.ENDC
        + " (%(name)s_%(lineno)d) (%(asctime)s) %(message)s"
    )
    logger = logging.getLogger()
    logger.setLevel(logLevel)
    console = logging.StreamHandler()
    console.setLevel(logLevel)
    console.setFormatter(logging.Formatter(console_format, datefmt="%d/%m %I:%M:%S %p"))
    logger.addHandler(console)


def get_happy_sign():
    return "\n> . < \n  O\n\  o  \  o\n \/    \/"


def get_bomb_sign():
    return "\n ,-*\n(_)"


def get_explosion_cloud():
    return "\n        --_--\n     (  -_    _).\n   ( ~       )   )\n (( )  (    )  ()  )\n  (.   )) (       )\n    ``..     ..``\n         | |\n       (=| |=)\n         | |       \n     (../( )\.))"


def get_library_sign():
    # https://patorjk.com/software/taag/#p=display&f=Graffiti&t=Lightning%20%0A%20%20%20Strikes%20%0A%20%20%20%20%20%20Twice
    return "\n.____    .__       .__     __         .__                 \n|    |   |__| ____ |  |___/  |_  ____ |__| ____    ____   \n|    |   |  |/ ___\|  |  \   __\/    \|  |/    \  / ___\  \n|    |___|  / /_/  >   Y  \  | |   |  \  |   |  \/ /_/  > \n|_______ \__\___  /|___|  /__| |___|  /__|___|  /\___  /  \n        \/ /_____/      \/          \/        \//_____/   \n     _________ __         .__ __                          \n    /   _____//  |________|__|  | __ ____   ______        \n    \_____  \\   __\_  __ \  |  |/ // __ \ /  ___/        \n    /        \|  |  |  | \/  |    <\  ___/ \___ \         \n   /_______  /|__|  |__|  |__|__|_ \\___  >____  >        \n           \/                     \/    \/     \/         \n      ___________       .__                               \n      \__    ___/_  _  _|__| ____  ____                   \n        |    |  \ \/ \/ /  |/ ___\/ __ \                  \n        |    |   \     /|  \  \__\  ___/                  \n        |____|    \/\_/ |__|\___  >___  >                 \n                                \/    \/                  \n    "


def get_angry_sign():
    return "( ｡ •̀ ᴖ •́ ｡)💢 "


def format_kafka_msg(msg, prefixStr=None):
    res = ""
    if prefixStr is not None:
        res = f"{prefixStr}: ->"
    res += "Topic: {} | Partition: {} | Offset: {} | Key: {}".format(
        msg.topic(), msg.partition(), msg.offset(), msg.key()
    )
    return res


def get_asterisk_long_delimiter():
    return '"***************************************************************"'


def get_crying_sign():
    return "(╥_╥)"


class BColors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    WHITE = "\033[37m"
    YELLOW = "\033[33m"
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"
    BLACK = "\033[30m"
    BHEADER = BOLD + "\033[95m"
    BOKBLUE = BOLD + "\033[94m"
    BOKGREEN = BOLD + "\033[92m"
    BWARNING = BOLD + "\033[93m"
    BFAIL = BOLD + "\033[91m"
    BUNDERLINE = BOLD + "\033[4m"
    BWHITE = BOLD + "\033[37m"
    BYELLOW = BOLD + "\033[33m"
    BGREEN = BOLD + "\033[32m"
    BBLUE = BOLD + "\033[34m"
    BCYAN = BOLD + "\033[36m"
    BRED = BOLD + "\033[31m"
    BMAGENTA = BOLD + "\033[35m"
    BBLACK = BOLD + "\033[30m"
