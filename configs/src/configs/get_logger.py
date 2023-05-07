import os
import logging
import time


def init_logger(log_filename):
    log_dir = f"log/{log_filename}"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    current_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())

    formatter = logging.Formatter('%(asctime)-15s %(message)s')

    file_handler = logging.FileHandler('{}/{}.log'.format(log_dir, log_filename))
    file_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(formatter)

    logger = logging.getLogger(log_filename)
    logger.addHandler(stderr_handler)
    logger.addHandler(file_handler)

    logger.setLevel(logging.INFO)

    return logger