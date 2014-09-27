#!/usr/bin/env python
"""Hadoop Kafka Consumer.

This uses https://github.com/miniway/kafka-hadoop-consumer to consume from Kafka
and output into HDFS.  Messages will be consumed into the HDFS path
<output_dir>/<topic>/<timestamp>.  Zookeeper server hosts will be
read out of the /etc/zookeeper/conf/zoo.cfg file.

Usage:
  kafka-hadoop-consume --topic=<topic> --group=<group> --output=<dir> [--regex] [--limit=<limit>] [--redirect-stderr] [--frequency=<frequency>]

Options:
  --help                    Show this help message and exit
  --topic=<topic>           Comma seprated list of Kafka topics from which to consume.  If --regex is given, then this is interpreted as a topic regular expression pattern.
  --group=<group>           Kafka consumer group to join
  --output=<dir>            HDFS directory in which to store output
  --regex                   If specified, topic will be interpreted as a regex, and matching topics in zookeeper will be consumed.
  --limit=<limit>           Number of messages to consume [default: -1] (-1 means no limit; messages will be read until the end of the stream.)
  --redirect-stderr         Redirects stderr into stdout.  This is useful if running in a cron job wrapped in cronic.
  --frequency=<frequency>   The interval in minutes between two consecutive generated Kafka datasets. [default: 15] and this parameter is supplied to standardize the timestamp within a filename. Set frequency to 0 to generate daily filenames without a time component.
  --queue=<queue_name>      Hadoop queue name (mapreduce.job.queuename). [default: standard]
"""

import os
import re
import sys

from datetime import datetime
from docopt import docopt

def get_topics(zookeeper_hosts, topic_regex):
    """Uses shell zookeeper-client to read Kafka topics matching topic_regex from ZooKeeper."""
    command        = "/usr/bin/zookeeper-client -server %s ls /brokers/topics | tail -n 1 | tr '[],' '   '" % ','.join(zookeeper_hosts)
    topics         = os.popen(command).read().strip().split()
    matched_topics = [ topic for topic in topics if re.match(topic_regex, topic) ]
    return matched_topics

def standardized_timestamp(frequency, dt=None):
    '''
    This function generates a timestamp with predictable minute and seconds
    components. Right now, we hardcode seconds to 0. The minutes component 
    is more interesting. For Oozie coordinator we need to have a predictable 
    timestamp component so we can predict how future input paths will look
    like. That's why we can't rely on a 'real' timestamp because the minutes
    and seconds are arbitrary.

    If dt is not given, then the a standarized timestamp based on the current
    time will be returned.  Else the standardized timestamp of dt will be
    returned.

    @param frequency (integer) that indicates how to collapse minutes. 
    For example frequency=15
    10:06 -> 10:00
    10:23 -> 10:15
    10:59 -> 10:45

    Frequency=30
    10:21 -> 10:00
    10:49 -> 10:30
    
    Frequency=0 is a special case used to generate daily filenames.
    2013-01-04 11:18 -> 2013-01-04
    2013-01-05 00:01 -> 2013-01-05
    '''

    if dt is None:
      dt = datetime.now() 

    frequency = int(frequency)
    # Special case were frequency=0 so we only return the date component
    if frequency == 0:
        return dt.strftime('%Y-%m-%d')

    blocks = 60 / frequency
    standardized_minutes = {}
    for block in xrange(blocks):
        standardized_minutes[block] = block * frequency

    collapsed_minutes = (dt.minute / frequency)
    minutes = standardized_minutes.get(collapsed_minutes, 0)
    timestamp = datetime(dt.year, dt.month, dt.day, dt.hour, minutes, 0)

    return timestamp.strftime('%Y-%m-%d_%H.%M.%S')

def consume_topic(zookeeper_hosts, topic, group, output_dir, limit, frequency, queue_name, redirect_stderr=False):
    print "Consuming %s messages from topic '%s' in consumer group %s into %s..." % (limit, topic, group, output_dir)
    timestamp = standardized_timestamp(frequency)
    output_path = "%s/%s/dt=%s" % (output_dir, topic, timestamp)
    os.system("/usr/bin/hadoop fs -mkdir %s" % output_path)
    command = "/usr/bin/kafka-hadoop-consumer -z %s -t %s -g %s -l %s -q %s -o %s" % (','.join(zookeeper_hosts), topic, group, limit, queue_name, output_path)
    # if specified, redirect the command's stderr
    # to stdout
    if redirect_stderr:
      command = command + " 2>&1"

    print command
    return os.system(command)

if __name__ == '__main__':
    # parse arguments
    arguments = docopt(__doc__)
    # print arguments
    group           = arguments['--group']
    limit           = arguments['--limit']
    output          = arguments['--output']
    regex           = arguments['--regex']
    topic           = arguments['--topic']
    redirect_stderr = arguments['--redirect-stderr']
    frequency       = arguments['--frequency']
    queue_name      = arguments['--queue']
    

    # read zookeeper hosts out of zoo.cfg
    zookeeper_conf_file = "/etc/zookeeper/conf/zoo.cfg"
    zookeeper_hosts = os.popen("grep server. %s | awk -F '=' '{print $2}' | awk -F ':' '{print $1}'" % zookeeper_conf_file).read().strip().split("\n")

    if regex:
        topics = get_topics(zookeeper_hosts, topic)
    else:
        topics = topic.split(',')

    print "\nConsuming topics: [%s] into Hadoop" % ', '.join(topics)

    exitval = 0
    for topic in topics:
        retval = consume_topic(zookeeper_hosts, topic, group, output, limit, frequency, queue_name, redirect_stderr)
        if (retval != 0):
            print "\nThere was an error encountered during consumption of topic %s" % topic
            exitval = retval

    # exit 0 if all topics were consumed,
    # exit 1 if there were any failures.
    sys.exit(exitval)