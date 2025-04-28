import sys
import os
import traceback


def get_exception_string(e=None):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    # print(exc_type, fname, exc_tb.tb_lineno)
    if e is None:
        return f"{traceback.format_exc()} {exc_type} {fname} {exc_tb.tb_lineno}"
    return f"{traceback.format_exc()} {exc_type} {fname} {exc_tb.tb_lineno} {e}"


class SelfDestructException(Exception):
    pass
