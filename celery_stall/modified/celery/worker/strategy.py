# -*- coding: utf-8 -*-
"""
    celery.worker.strategy
    ~~~~~~~~~~~~~~~~~~~~~~

    Task execution strategy (optimization).

"""
from __future__ import absolute_import

from .job import Request

import logging
from celery.utils.log import get_logger
import os
logger = get_logger(__name__)
logger_task = logging.getLogger(__name__)
logger_task.setLevel(logging.DEBUG)
fh = logging.handlers.RotatingFileHandler("/var/log/celery/task.log", 'a', maxBytes=52428800, backupCount=50)
formatter = logging.Formatter('[%(asctime)s %(levelname)s/%(name)s] %(message)s')
fh.setFormatter(formatter)
logger_task.addHandler(fh)


def default(task, app, consumer):
    hostname = consumer.hostname
    eventer = consumer.event_dispatcher
    Req = Request
    handle = consumer.on_task
    connection_errors = consumer.connection_errors

    def task_message_handler(message, body, ack):
        req = Req(body, on_ack=ack, app=app, hostname=hostname,
                   eventer=eventer, task=task,
                   connection_errors=connection_errors,
                   delivery_info=message.delivery_info)
        logger_task.info("anan: Consumer-{} Received task: {{task_id:{}, task_name:{}, worker:{}}}"\
                         .format(os.getpid(), req.id, req.name, req.worker_pid))
        logger.info("Received task: {}".format(req))
        handle(Req(body, on_ack=ack, app=app, hostname=hostname,
                   eventer=eventer, task=task,
                   connection_errors=connection_errors,
                   delivery_info=message.delivery_info))
    return task_message_handler
