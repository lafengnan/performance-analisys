#!/usr/bin/env python
# coding=utf-8
from celery import Celery

BROKER_URL = 'amqp://guest:guest@192.168.20.37;amqp://guest:guest@192.168.20.46'

class Monitor(object):
    """
    Monior
    Monitoring the job status of celery
    @app: the celery app
    """
    events = ('task-sent', 'task-received', 'task-started',
              'task-succeeded', 'task-failed', 'task-retried',
              'task-revoked')

    def __init__(self, app, **kwargs):
        super(Monitor, self).__init__()
        self.app = app
        self.kw = kwargs
        self.state = self.app.events.State()

    def _task_sent(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        print('TASK SENT: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_received(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        print('TASK RECEIVED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_started(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        print('TASK STARTED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_succeeded(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        print('TASK SUCCEEDED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_failed(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        print('TASK FAILED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_retried(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        print('TASK RETRIED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def _task_revoked(self, event):
        self.state.event(event)
        task = self.state.tasks.get(event['uuid'])
        print('TASK REVOKED: %s[%s] %s' % (
            task.name, task.uuid, task.info(), ))

    def __call__(self):
        handlers = {k:getattr(self, "_%s"%k.replace('-','_')) for k in self.events}
        with self.app.connection() as connection:
            recv = app.events.Receiver(connection, handlers=handlers)
            recv.capture(limit=None, timeout=None, wakeup=True)

if __name__ == '__main__':
    app = Celery(broker=BROKER_URL, ssl=True)
    Monitor(app)()
