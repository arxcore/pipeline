# FIXME:
# learning hierarcky exception handling lifecyle
import logging
from base_exc import (
    PipelineCrash,
    ProcessingFailed,
    RoutingError,
    FetchDataError,
    BLSRequestsError,
)

logger = logging.getLogger(__name__)


def t():
    try:
        logger.info("exception test")
        a = ["1"]
        a[1]
    except IndexError as e:
        raise BLSRequestsError(e)


def y():
    try:
        t()
        a = {"1": 0}
        a["1"]

    except KeyError as e:
        raise FetchDataError(e)
    except FetchDataError as e:
        logger.error("FetchDataError %s", str(e))
        raise


def x():
    try:
        y()
        # 1 / 0
    except ZeroDivisionError as e:
        raise RoutingError(e)
    except RoutingError as e:
        logger.error("RoutingError %s", str(e))
        raise


def z():
    try:
        x()
        # "1" / 5
    except TypeError as e:
        raise ProcessingFailed(e)
    except ProcessingFailed as e:
        logger.error("Procesing Failed during operation %s", str(e))
        raise


def f():

    data = [0]
    for x in data:
        if x == 0:
            print(x)
            return x
        print("hm")


def main():
    try:
        f()
    # helo word("print")
    except SyntaxError as e:
        raise PipelineCrash(e)
    except PipelineCrash as e:
        logger.error("error pipeline %s", str(e))
        raise


if __name__ == "__main__":
    main()
