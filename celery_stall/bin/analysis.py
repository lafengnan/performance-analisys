#!/usr/bin/env python
# coding=utf-8

import re
import os
import sys
import csv
import time
import pandas as pd
import webbrowser

from optparse import OptionParser
from collections import OrderedDict
from functools import wraps

Commands = ("build", "read")

USAGE = """
%prog <command> [options]
Commands:
""" + '\n'.join(["%10s: " % x for x in Commands])


def timing(f, *args, **kwargs):
    @wraps(f)
    def deco(*args, **kwargs):
        b = time.time()
        r = f(*args, **kwargs)
        e = time.time()
        print("\nCost {} seconds!".format(e-b))
        return r
    return deco

class Analyzer(object):
    """
    Analyzer is used to analyze the celery log by convert the
    key data to csv format, and then use pandas to analyze the
    data.
    @log: the log file to analyze
    @type: the report type:[csv]
    @out: the output format
    """
    _csv_execution_fields = ('start', 'end', 'queue', 'task_id',
                             'task_name', 'duration', 'retries')
    _csv_inqueue_fields = ('task_id', 'task_name', 'queue',
                           'inq', 'received', 'duration', 'outq',
                           'latency', 'retries')


    def __init__(self, data=None, log='celery.log', type="csv", *args, **kwargs):
        super(Analyzer, self).__init__()
        self._data = data
        self._log = log
        self._report_dir = "reports"
        self._type = type or "csv"
        self._name_format = "{name}.{type}" if data == 'worker' \
            else "{name}_queue.{type}"
        self._stat_report_name_format = "{name}_stat.{suffix}" if data== 'worker' \
            else "{name}_queue_stat.{suffix}"
        self._out_file = self._name_format.format(name=log.split(".")[0],
                                                           type=type)
        self._html_report = self._name_format.format(name=log.split(".")[0],
                                                     type="html")
        self._stat_report = self._stat_report_name_format.format(name=log.split(".")[0],
                                                     suffix="html")
        self._args = args
        self._kwargs = kwargs
        self._header = self._csv_inqueue_fields if self._data == 'queue' \
        else self._csv_execution_fields
        self._writer = self._build_csv_writer(self._out_file, self._header)

    @property
    def html_report(self):
        return self._html_report

    @html_report.setter
    def html_report(self, new_name):
        self._html_report = new_name

    def _build_csv_writer(self, csv_f, header):
        if not os.path.exists(self._report_dir):
            os.mkdir(self._report_dir)
        self._out_file = os.path.join(self._report_dir, os.path.basename(csv_f))
        def _build_csv_header():
            try:
                with open(self._out_file, 'wb') as f:
                    w = csv.DictWriter(f, header)
                    w.writeheader()
            except IOError:
                raise
        try:
            _build_csv_header()
            try:
                self._csv_fd = open(self._out_file, 'a')
                return csv.DictWriter(self._csv_fd, self._header)
            except IOError:
                raise
        except Exception:
            raise


    def _write_out(self, data):
        self._writer.writerow(data)

    def _collect_in_queue_lines(self, line, out_lines, pattern_tx="publishing",
                               pattern_rx="(anan)+.*Received", sep=' '):
        _tmp_lines = out_lines
        prog_tx = re.compile(pattern_tx)
        prog_rx = re.compile(pattern_rx)
        prog_accept = re.compile("(anan)+.*task-accepted")

        mt_tx = prog_tx.search(line)
        mt_rx = prog_rx.search(line)
        mt_accept = prog_accept.search(line)

        if mt_tx:
            data = line.rstrip('\n').split(sep)
            date, ts, q, task_id, task_name, retries = \
                data[0].lstrip('['),\
                data[1].rstrip(':'), \
                data[-2].split(':')[1].split('.')[0], \
                data[-3].split(':')[1].rstrip(','), \
                data[-2].split(':')[1].split('.')[-1].rstrip(','),\
                data[-1].split(':')[1].rstrip('}')
            _d = dict({'task_id':task_id, 'task_name':task_name,
                       'queue':q, 'inq':"{} {}".format(date, ts),
                       'received':None, 'duration':None, 'outq':None,
                       'latency':None, 'retries':retries
                       })
            if task_id in _tmp_lines:
                for k in _d:
                    if _d[k] != _tmp_lines[task_id][k] and _d[k]:
                        _tmp_lines[task_id][k] = _d[k]
            else:
                # add to dict
                _tmp_lines[task_id] = _d
        if mt_rx:
            data = line.rstrip('\n').split(sep)
            date, ts, q, task_id, task_name, retries = \
                data[0].lstrip('['), \
                data[1].rstrip(':'), \
                data[-2].split(':')[1].split('.')[0], \
                data[-3].split(':')[1].rstrip(','), \
                data[-2].split(':')[1].split('.')[-1].rstrip(','),\
                0

            _d = dict({'task_id':task_id, 'task_name':task_name,
                       'queue':q, 'inq':None,
                       'received':"{} {}".format(date, ts),
                       'duration': 0,
                       'outq':None,
                       'latency': 0, 'retries':retries})

            if task_id in _tmp_lines:
                _tmp_lines[task_id]['received'] = _d['received']

                if _tmp_lines[task_id]['inq']:
                    in_time_str = _tmp_lines[task_id]['inq']
                    o_time_str = _d['received']
                    in_time = time.mktime(time.strptime(in_time_str.split(',')[0],
                                                        "%Y-%m-%d %H:%M:%S")) + \
                        float('0.%s'%in_time_str.split(',')[1])
                    o_time = time.mktime(time.strptime(o_time_str.split(',')[0],
                                                       "%Y-%m-%d %H:%M:%S")) + \
                        float('0.%s'%o_time_str.split(',')[1])
                    _tmp_lines[task_id]['duration'] = _d['duration'] = o_time - in_time
            else:
                # add to dict
                _tmp_lines[task_id] = _d
        if mt_accept:
            data = line.rstrip('\n').split(sep)
            date, ts, q, task_id, task_name, retries = \
                data[0].lstrip('['), \
                data[1].rstrip(':'), \
                data[-1].split(':')[1].split('.')[0], \
                data[-2].split(':')[1].rstrip(','), \
                data[-1].split(':')[1].split('.')[-1].rstrip('}'),\
                0

            _d = dict({'task_id':task_id, 'task_name':task_name,
                       'queue':q, 'inq':None, 'received':None,
                       'duration': 0,
                       'outq':"{} {}".format(date, ts),
                       'latency': 0, 'retries':retries})

            if task_id in _tmp_lines:
                _tmp_lines[task_id]['outq'] = _d['outq']

                if _tmp_lines[task_id]['received']:
                    in_time_str = _tmp_lines[task_id]['received']
                    o_time_str = _d['outq']
                    in_time = time.mktime(time.strptime(in_time_str.split(',')[0],
                                                        "%Y-%m-%d %H:%M:%S")) + \
                        float('0.%s'%in_time_str.split(',')[1])
                    o_time = time.mktime(time.strptime(o_time_str.split(',')[0],
                                                       "%Y-%m-%d %H:%M:%S")) + \
                        float('0.%s'%o_time_str.split(',')[1])
                    _tmp_lines[task_id]['latency'] = _d['latency'] = o_time - in_time
                # output the data and remove the item
                self._write_out(_tmp_lines[task_id])
                _tmp_lines.pop(task_id)
            else:
                # add to dict
                _tmp_lines[task_id] = _d

    def _queue(self, line, o_data):
        self._collect_in_queue_lines(line, o_data)

    def _worker(self, line, out_lines=None, pattern="spends", sep=' '):
        start_pattern = re.compile("starts executing")
        end_pattern = re.compile(pattern)
        end_mt = end_pattern.search(line)
        start_mt = start_pattern.search(line)
        if start_mt:
            data = line.rstrip('\n').split(sep)
            start, q, task_id, task_name, retries \
                = \
                "{} {}".format(data[0].lstrip('['), data[1].rstrip(':')), \
                data[9].split(':')[1].split('.')[0], \
                data[8].split(':')[1].rstrip(','), \
                data[9].split(':')[1].split('.')[-1].rstrip(','), \
                data[-1].split(':')[1].rstrip('}')

            _d = {'start': start, 'end': None, 'queue':q, 'task_id':task_id,
                  'task_name': task_name, 'duration': None, 'retries': retries}
            out_lines[task_id] = _d
        if end_mt:
            data = line.rstrip('\n').split(sep)
            end, q, task_id, task_name, tv, retries\
                = \
                "{} {}".format(data[0].lstrip('['), data[1].rstrip(':')),\
                data[11].split(':')[1].split('.')[0],\
                data[10].split(':')[1].rstrip(','), \
                data[11].split(':')[1].split('.')[-1].rstrip(','),\
                data[6], \
                data[-1].split(':')[1].rstrip('}')
            if task_id in out_lines:
                out_lines[task_id]['end'] = end
                out_lines[task_id]['duration'] = tv
                self._write_out(out_lines[task_id])
                out_lines.pop(task_id)
                #wd = {self._csv_execution_fields[i]:data_new[i] for i in xrange(len(data_new))}
            else:
                out_lines[task_id] = {'start': None, 'end': end, 'queue':q, 'task_id':task_id,
                  'task_name': task_name, 'duration': tv, 'retries': retries}
    @timing
    def _build_report(self):
        """
        out_data = {
        'task_id': {'task_id':xxx, 'task_name':xxxx}
        }
        """

        func = getattr(self, "_%s"%self._data)
        out_data = OrderedDict()
        try:
            with open(self._log, 'r') as f:
                for l in f:
                    try:
                        func(l, out_data)
                    except Exception as e:
                         print("build report failed for {}".format(e))
                         raise
        except IOError as e:
            print("Open file:{} error {}".format(self._log, e))
            raise
        # The remainders are not completed tasks or error retries, Just ingnore
        #for v in out_data.values():
        #    self._write_out(v)
        self._csv_fd.close()
        if self._kwargs.get('html'):
            self.create_html_report()
    @timing
    def read_csv_report(self, csv, idx, num, asc=False, bar_len=0):
        csv = self._kwargs['report'] or csv
        #df = pd.read_csv(os.path.join(self._report_dir, csv))
        df = pd.read_csv(csv)
        if idx == 'index':
            print df.sort(ascending=asc).head(num)
        else:
            print df.sort(idx, ascending=asc).head(num)
        print bar_len*"="
        print "%80s"%"Statistic Info"
        print bar_len*"="
        print df.describe()
        print bar_len*"="
        print df.task_name.describe()


    def create_html_report(self):
        pf = pd.read_csv(self._out_file)
        self.html_report = os.path.join(self._report_dir,
                                        os.path.basename(self.html_report))
        try:
            with open(self.html_report, 'wb') as f:
                f.writelines(pf.to_html())
        except IOError:
            raise

    def draw_duration_plot(self):
        pass

    def run(self):
        try:
            print("Starts analyzing...")
            b = time.time()
            self._build_report()
            spends = time.time() - b
            reports = (self._out_file, self.html_report) \
                if self._kwargs.get('html') \
                else (self._out_file, 'Not create html report')
            print ("Report Generated:\n\t{1}\n\t{2}"\
                   .format(spends, reports[0], reports[1]))
        except:
            raise

def main():

    parser = OptionParser(USAGE)
    parser.add_option('-a', action="store_true", dest="asc", default=False,
                      help="display the records in ascending or descending")
    parser.add_option('-b', action='store_true', dest='browser',
                      default=False, help='Open report in browser or not')
    parser.add_option('-d', '--data', type="string", dest="data", default='worker',
                      help="config which data to analyze[worker|queue]")
    parser.add_option('-w', action='store_true', dest='html',
                      default=False, help='create html webpage report')
    parser.add_option('-i', '--index', type='string', dest='idx', default='duration',
                      help='display by which index[duration, latency]')
    parser.add_option('-l', '--log', type='string', dest='log', default='celery.log',
                      help='the log name which will be analyzed')
    parser.add_option('-n', '--number', type="int", dest="number", default=50,
                      help="config the number of records to show")
    parser.add_option('-r', '--report', type='string', dest='report', default='celery.csv',
                      help='the csv report to read')


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

    try:
        if cmd == 'build':
            analyzer = Analyzer(options.data, options.log, html=options.html)
            analyzer.run()
        else:
            analyzer = Analyzer(report=options.report)
            print("Starts reading report...")
            BAR_LEN = 195 if 'queue' in options.report\
                 else 145 if options.data == 'worker' \
                 else 0
            print BAR_LEN*"="
            analyzer.read_csv_report(
                                 options.report,
                                 options.idx,
                                 options.number,
                                 options.asc,
                                 BAR_LEN)
            #analyzer.draw_duration_plot()
            print BAR_LEN*"="
        if options.browser:
            webbrowser.open(analyzer.report)
    except Exception as e:
        print("analyzing failed! Reason: {}".format(e))

if __name__ == '__main__':
    sys.exit(main())
