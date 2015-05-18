#!/usr/bin/env python
# coding=utf-8

import os
import sys
import resource
import time
import gc
import csv
import re
from functools import wraps
from collections import OrderedDict
from memory_profiler import profile
import pandas as pd
from optparse import OptionParser
try:
    from wand.image import Image
except ImportError:
    pass


Commands = ("analyze", "read", "profiling")

USAGE = """
%prog <command> [options]
Commands:
""" + '\n'.join(["%10s: " % x for x in Commands])

def timeit(f, *args, **kwargs):
    @wraps(f)
    def deco(*args, **kwargs):
        b = time.time()
        f(*args, **kwargs)
        e = time.time()
        #if DEBUG:
        #    print("{0} seconds...".format(e-b))
    return deco

#@timeit
@profile
def profile_wand(*args, **kwargs):
    path = kwargs['path']
    with Image(filename=path) as original:
        with original.clone() as resized:
            resized.transform(resize=("256x256"))
            resized.format='jpeg'

class Profiler(object):
    """
    Profiling wand memory usage
    """
    csv_header = ('count', 'memory')

    def __init__(self, csv, dir, callback=profile_wand, *args, **kwargs):
        super(Profiler, self).__init__()
        self._test_dir = dir
        self._report_dir = "reports"
        self._callback = callback
        self._args = args
        self._kwargs = kwargs
        self._mem_stats = OrderedDict()
        self._count = 0
        self.does_info = kwargs['info'] or False
        self._w = self._build_csv_writer(csv)

    def _mem_usage_kb(self):
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    def _build_csv_header(self, csv_file):
        if not os.path.exists(self._report_dir):
            os.mkdir(self._report_dir)
        self._csv_file = os.path.join(self._report_dir, csv_file)
        try:
            with open(self._csv_file, 'wb') as f:
                w = csv.DictWriter(f, self.csv_header)
                w.writeheader()
        except IOError as e:
            raise

    def _build_csv_writer(self, csv_file):
        self._build_csv_header(csv_file)
        try:
            self.csv_fd = open(self._csv_file, 'a')
            w = csv.DictWriter(self.csv_fd, self.csv_header)
        except IOError:
            raise
        return w

    def travel_path(self):
        for f in os.listdir(self._test_dir):
            f_path = os.path.join(self._test_dir, f)
            print "{0} starts processing {1}".format(self._callback.__name__, f_path)
            self._callback(path=f_path)
            self._w.writerow({'count':str(self._count), 'memory':self._mem_usage_kb()})
            if self.does_info:
                print "Memory is used: %d KB" % self._mem_usage_kb()
            self._count += 1

    def info(self):
        print "%d file are handled." % self._count
        gc.collect()
        print("garbage stats:{}".format(gc.garbage))

class TopLogAnalyzer(object):
    """
    Analyze the top command output for long time monitor
    Will create a csv report for graphic display
    @log: the top command output by using top -p [pid1, pid2, ...] -b > top.log
    @pids_num: the number of processes to monitor
    @user: the user of the pids

    """
    csv_header = ('timestamp', 'load_1', 'load_5', 'load_15',
                  'pid', 'virt', 'res', 'shr', 'cpu', 'mem'
                  )
    def __init__(self, log=None, pids=0, user='ubuntu', *args, **kwargs):
        super(TopLogAnalyzer, self).__init__()
        self._log = log
        self._report_dir = 'reports'
        self._pids = pids
        self._user = user
        self._args = args
        self._kwargs = kwargs
        self._csv = log.split('.')[0] + \
            "_{}.csv".format(time.strftime('%y%m%d%H%M%S')) if log \
            else "top_{}.csv".format(time.strftime('%y%m%d%H%M%S'))
        self._csv_writer = self._build_csv_writer() if kwargs['cmd'] == 'analyze' else None


    def __enter__(self):
        return self

    def __exit__(self, exc_t, exc_v, tb):
        self._fd.close()

    def _build_csv_writer(self):
        def _build_header():
            if not os.path.exists(self._report_dir):
                os.mkdir(self._report_dir)
            self._csv = os.path.join(self._report_dir, self._csv)
            try:
                with open(self._csv, 'wb') as f:
                    w = csv.DictWriter(f, self.csv_header)
                    w.writeheader()
            except IOError:
                raise
        try:
            _build_header()
        except:
            raise
        try:
            self._fd = open(self._csv, 'a')
            w = csv.DictWriter(self._fd, self.csv_header)
        except IOError:
            raise
        return w

    def _collect_data(self):

        def _collect_top_line(line):
            words = line.rstrip('\n').split(' ')
            # timestamp, load_1, load_5, load_15
            headline_data = [words[2], words[-3].rstrip(','), words[-2].rstrip(','), words[-1]]
            return headline_data

        def _collect_stats(line):
            words = [x for x in line.rstrip('\n').rstrip(' ').lstrip(' ').split(' ') if x != '']
            # pid, virt, res, shr, %cpu, %mem
            pid = words[0]
            virt = words[4].rstrip('m')
            res = words[5].rstrip('m')
            shr = words[6]
            cpu = words[8]
            mem = words[9]
            return [pid, virt, res, shr, cpu, mem]

        _loads_pattern = re.compile("load")
        _stats_pattern = re.compile(self._user)

        try:
            with open(self._log, 'r') as f:
                _stats = list()
                for l in f:
                    if  _loads_pattern.search(l):
                        _stats = [] # reset _stats for new interval
                        _stats +=  _collect_top_line(l)
                    elif _stats_pattern.search(l):
                        pid_stat = _collect_stats(l)
                        _stats += pid_stat
                        data = dict(zip(self.csv_header, _stats))
                        self._csv_writer.writerow(data)
                        # Drain out pid_stat for next pid
                        for x in pid_stat:
                            _stats.remove(x)
        except IOError:
            raise

    def analyze(self, *args, **kwargs):
        print("Starts building csv report...")
        self._collect_data()
        print("Done!\nReport Generated:\n\t{}".format(self._csv))

    def read_csv_report(self, csv_report, idx, num, asc=False):
        print("\nreading {}...\n".format(csv_report))
        #path = os.path.join(os.getcwd(), csv_report)
        path = os.path.join(os.curdir, csv_report)
        df = pd.read_csv(path)
        print 75*"="
        if idx == 'index':
            print df.sort(ascending=asc).head(num)
        else:
            print df.sort(idx, ascending=asc).head(num)
        print 75*"="
        print "%40s"%"Statistic Info"
        print "%45s"%(12*"==")
        print df.describe()
        print 75*"="



def profiling(target_dir, loops, verbose=False):

    p = Profiler("report_{}.csv".format(time.strftime('%y%m%d%M%S')),
                 target_dir, info=verbose)

    for i in xrange(1, loops + 1):
        print("Round {0}......".format(i))
        p.travel_path()
        if p.does_info:
            p.info()
    print("Test Done! Building Report......")


def main():

    parser = OptionParser(USAGE)

    parser.add_option('-a', action="store_true", dest="asc", default=False,
                      help="display the records in ascending or descending")
    parser.add_option('-c', '--count', type="int", dest="count", default=1,
                      help='The test loops count')
    parser.add_option('-d', '--dir', type="string", dest="dir", default='1000_pics',
                      help='The directory contains test files')
    parser.add_option('-i', '--index', type='string', dest='idx', default='virt',
                      help='display by which index[cpu|mem|load_1~load_5|virt|res|shr]')
    parser.add_option('-l', '--log', type="string", dest="log", default='top.log',
                      help='The log to analzye')
    parser.add_option('-n', '--number', type='int', dest="num",
                      help='The number of pids to monitor or number of records to display')
    parser.add_option('-r', '--report', type='string', dest="report", default='top.csv',
                      help='The csv report to read')
    parser.add_option('-u', '--user', type="string", dest="user", default='ubuntu',
                      help='The user who runs the pids')
    parser.add_option('-v', '--verbose', action='store_false', dest='verbose',
                      default=False, help='print more details')

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

    if cmd == 'analyze':
        parameters = (options.log, options.num, options.user)
        with TopLogAnalyzer(*parameters, cmd='analyze') as analyzer:
            analyzer.analyze()
    elif cmd == 'read':
        analyzer = TopLogAnalyzer(cmd='read')
        analyzer.read_csv_report(options.report, options.idx, options.num, options.asc)
    elif cmd == 'profiling':
        profiling(options.dir, options.count, options.verbose)



if __name__ == "__main__":
    sys.exit(main())

