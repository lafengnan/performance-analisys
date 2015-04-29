#!/usr/bin/env python
# coding=utf-8

import sys
import inspect
from optparse import OptionParser

from celery import Celery
import logging
from logging import FileHandler, StreamHandler



BROKER_URL = 'amqp://guest:guest@192.168.20.37;amqp://guest:guest@192.168.20.46'

Commands = ("monitor", "dump")

USAGE = """
%prog <command> [options]
Commands:
""" + '\n'.join(["%10s: " % x for x in Commands])

class Stat(object):
    """
    Stat
    Collect stats info
    """
    items = ('sent', 'received', 'started', 'succeeded', 'failed', 'retried', 'revoked')

    def __init__(self, report="report.csv", **kwargs):
        super(Stat, self).__init__()
        self.reset()

    def reset(self):
        self.stat = {k:0 for k in self.items}

    def incr(self, k):
        assert k in self.items
        def deco(f, *args, **kwargs):
            def real_deco(*args, **kwargs):
                f(*args, **kwargs)
                self.stat[k] += 1
            return real_deco
        return deco

    def decr(self, k):
        assert k in self.items
        self.stat[k] -= 1

    def report(self):
        pass

class Monitor(object):
    """
    Monior
    Monitoring the job status of celery
    @app: the celery app
    """
    events = ('task-sent', 'task-received', 'task-started',
              'task-succeeded', 'task-failed', 'task-retried',
              'task-revoked')

    def __init__(self, app, logger, **kwargs):
        super(Monitor, self).__init__()
        self.app = app
        self.kw = kwargs
        self.state = self.app.events.State()
        self.stats = kwargs.get('stat', Stat())
        self.event_to_monitor = kwargs.get('event', 'all')
        self.tasks= kwargs.get('task', 'all')
        self.will_monitor_specific_task = True if self.tasks != 'all' else False
        self.verbose = kwargs.get('verbose', False)
        self.logger = logger

    def _get_func_name(self):
        return inspect.stack()[1][3]

    def _task_sent(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        if self.will_monitor_specific_task and self.tasks == task.name \
                or not self.will_monitor_specific_task:
            self.logger.info('TASK SENT: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_received(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        if self.will_monitor_specific_task and self.tasks == task.name \
                or not self.will_monitor_specific_task:
            self.logger.info('TASK RECEIVED: %s[%s] %s' % (
                task.name, task.uuid, task.info(), ))

    def _task_started(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        if self.will_monitor_specific_task and self.tasks == task.name \
                or not self.will_monitor_specific_task:
            self.logger.info('TASK STARTED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_succeeded(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        if self.will_monitor_specific_task and self.tasks == task.name \
                or not self.will_monitor_specific_task:
            self.logger.info('TASK SUCCEEDED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_failed(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        if self.will_monitor_specific_task and self.tasks == task.name \
                or not self.will_monitor_specific_task:
            self.logger.info('TASK FAILED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_retried(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        if self.will_monitor_specific_task and self.tasks == task.name \
                or not self.will_monitor_specific_task:
            self.logger.info('TASK RETRIED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_revoked(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        if self.will_monitor_specific_task and self.tasks == task.name \
                or not self.will_monitor_specific_task:
            self.logger.info('TASK REVOKED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def __call__(self):
        if self.event_to_monitor == 'all':
            events = self.events
        else:
            events = [self.event_to_monitor]
        handlers = {k:getattr(self, "_%s"%k.replace('-','_')) for k in events}
        if self.verbose:
            self.logger.info("Monitoring {e} for {t} task...".format(e=events, t=self.tasks))
        with self.app.connection() as connection:
            recv = self.app.events.Receiver(connection, handlers=handlers)
            recv.capture(limit=None, timeout=None, wakeup=True)


def setup_logger(log=False):
    FORMAT = '[%(asctime)-15s] %(message)s'
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handlers = [StreamHandler(stream=sys.stdout)]
    if log:
        handlers.append(FileHandler("monitor.log"))
    format = logging.Formatter(FORMAT)
    for h in handlers:
        h.setFormatter(format)
        logger.addHandler(h)

    return logger

def print_conf(logger, options):
    print("\nStart monitor...")
    log = "monitor.log" if options.log else None
    print("\nlog file:{}\n".format(log))

def main():

    parser = OptionParser(USAGE)
    parser.add_option('-e', '--event', type="string", dest="event", default="all",
                      help="The event to monitoring")
    parser.add_option('-l', action='store_true', dest='log', default=False,
                      help='the log to store info')
    parser.add_option('-r', '--report', type='string', dest='report', default='status.csv',
                      help='the csv report to read')
    parser.add_option('-t', '--task', type='string', dest='task', default='all',
                      help='The task to monitoring')
    parser.add_option('-v', action='store_true', dest='verbose', default=False,
                      help='verbose mode')

    options, args = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        print "Error: config the command"
        return 1

    cmd = args[0]
    if cmd not in Commands:
        parser.print_help()
        print "Error: Unkown command: ", cmd
        return 1

    app = Celery(broker=BROKER_URL, ssl=True)
    logger = setup_logger(options.log)

    print_conf(logger, options)
    Monitor(app, logger, event=options.event, task=options.task, verbose=options.verbose)()

if __name__ == '__main__':
    sys.exit(main())
