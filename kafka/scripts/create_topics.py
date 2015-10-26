#!/usr/bin/python

import sys
import json
import os
import time

import subprocess

def execute_command(*command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    res = ""
    while True:
        line = proc.stdout.readline()
        if line != '':
            res += line
        else:
            break
    return res

def run():
    topics_string = os.environ.get('KAFKA_CREATE_TOPICS')
    if topics_string is None:
        print "No topics config passed"
        return

    if len(sys.argv) > 1:
        try:
            time.sleep(int(sys.argv[1]))
        except ValueError:
            print "Incorrect value for timeout"
            exit(1)

    topics = topics_string.split(",")

    for topic in topics:
        try:
            topic_name, replication_factor, partitions = topic.split(":")
        except ValueError:
            print "Failed to decode topic config %s" % topic
            continue

        for _ in xrange(10):
            res = execute_command(os.path.join(os.environ.get('KAFKA_HOME'), 'bin/kafka-topics.sh'), 
                "--create",
                "--zookeeper", "localhost:2181",
                "--replication-factor", replication_factor,
                "--partitions", partitions,
                "--topic", topic_name)
            if "Created topic \"%s\"." % topic_name in res or "already exists" in res:
                break
            else:
                print "Failed to create topic %s, retrying in 0.5 seconds" % res
                time.sleep(0.5)

if __name__ == "__main__":
    run()

