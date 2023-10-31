import logging
import logging.handlers


def set_logger(module_name):
    logger = logging.getLogger(module_name)
    logger.handlers.clear()
    logger.propagate = False  # logが2重に出ないようにpropagareを設定

    streamHandler = logging.StreamHandler()

    formatter = logging.Formatter(
        "[%(levelname)s] - %(funcName)s - %(message)s")

    streamHandler.setFormatter(formatter)

    logger.setLevel(logging.DEBUG)
    streamHandler.setLevel(logging.DEBUG)

    logger.addHandler(streamHandler)

    return logger
