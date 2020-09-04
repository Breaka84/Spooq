"""
Global Logger instance used by Spooq2.

Example
-------
>>> import logging
>>> logga = logging.getLogger("spooq2")
<logging.Logger at 0x7f5dc8eb2890>
>>> logga.info("Hello World")
[spooq2] 2020-03-21 23:55:48,253 INFO logging_example::<module>::4: Hello World

"""

import os
import sys
import logging

initialized = False


def initialize():
    """
    Initializes the global logger for Spooq with pre-defined levels for ``stdout`` and ``stderr``.
    No input parameters are needed, as the configuration is received via :py:meth:`get_logging_level`.

    Note
    ----
    The output format is defined as:
        | "[%(name)s] %(asctime)s %(levelname)s %(module)s::%(funcName)s::%(lineno)d: %(message)s"
        | For example "[spooq2] 2020-03-11 15:40:59,313 DEBUG newest_by_group::__init__::53: group by columns: [u'user_id']"

    Warning
    -------
    The ``root`` logger of python is also affected as it has to have a level at least as
    fine grained as the logger of Spooq, to be able to produce an output.
    """
    global initialized
    if initialized:
        return

    logging_level = get_logging_level()

    # logging.getLogger("root").setLevel(logging_level)
    logger = logging.getLogger("spooq2")
    logger.setLevel(logging_level)

    if not len(logger.handlers):
        formatter = logging.Formatter(
            "[%(name)s] %(asctime)s %(levelname)s %(module)s::%(funcName)s::%(lineno)d: %(message)s"
        )

        # STDOUT Handler
        ch_out = logging.StreamHandler(sys.stdout)
        ch_out.setLevel(logging_level)
        ch_out.setFormatter(formatter)
        logger.addHandler(ch_out)

        # STDERR Handler
        # ch_err = logging.StreamHandler(sys.stderr)
        # ch_err.setLevel(logging_level)
        # ch_err.setFormatter(formatter)
        # logger.addHandler(ch_err)

        initialized = True


def get_logging_level():
    """
    Returns the logging level depending on the environment variable `SPOOQ_ENV`.

    Note
    ----
    If SPOOQ_ENV is
        * **dev**        -> "DEBUG"
        * **test**       -> "ERROR"
        * something else -> "INFO"

    Returns
    -------
    :any:`str`
        Logging level
    """
    spooq_env = os.getenv('SPOOQ_ENV', "default").lower()
    if spooq_env.startswith("dev"):
        return "DEBUG"
    elif spooq_env.startswith("test"):
        return "ERROR"
    elif spooq_env.startswith("pr"):
        return "WARN"
    else:
        return "INFO"
