from __future__ import division
from __future__ import print_function

import argparse
import logging
import re
import sys
from functools import partial
from multiprocessing import Pool
from multiprocessing import Process
from operator import itemgetter

import paramiko
from kafka import KafkaClient
from yelp_kafka import discovery
from yelp_kafka.error import ConfigurationError

from yelp_kafka_tool.util.zookeeper import ZK


DATA_PATH = "/nail/var/kafka/md1/kafka-logs"
JAVA_HOME = "/usr/lib/jvm/java-8-oracle-1.8.0.20/"

DEFAULT_BATCH_SIZE = 5
IONICE = "ionice -c 3"

FIND_MINUTES_COMMAND = 'find "{data_path}" -type f -name "*.log" -mmin -{minutes}'
FIND_START_COMMAND = 'find "{data_path}" -type f -name "*.log" -newermt "{start_time}"'
FIND_RANGE_COMMAND = 'find "{data_path}" -type f -name "*.log" -newermt "{start_time}" \! -newermt "{end_time}"'
CHECK_COMMAND = 'JAVA_HOME="{java_home}" {ionice} kafka-run-class kafka.tools.DumpLogSegments --files "{files}"'
REDUCE_OUTPUT = 'grep -v "isvalid: true"'

TIME_FORMAT_REGEX = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$")
TP_FROM_FILE_REGEX = re.compile(".*\\/kafka-logs\\/(.*-[0-9]+).*")

FILE_PATH_REGEX = re.compile("Dumping (.*)")
VALID_MESSAGE_REGEX = re.compile(".* isvalid: true")
INVALID_MESSAGE_REGEX = re.compile(".* isvalid: false")
INVALID_BYTES_REGEX = re.compile(".*invalid bytes")


def chunks(l, n):
    """Yield successive n-sized chunks from l.

    :param l: the list
    :type l: list
    :param n: the size of the chunk
    :type n: int
    :returns: a sequence of n-sized chunks of the input list
    :rtype: generator
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def ssh_client(host):
    """Start an ssh client.

    :param host: the host
    :type host: str
    :returns: ssh client
    :rtype: Paramiko client
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host)
    return ssh


def parse_opts():
    parser = argparse.ArgumentParser(
        description=('Performs a rolling restart of the specified'
                     'kafka cluster.'))
    parser.add_argument(
        '--cluster-type',
        required=True,
        help='cluster type, e.g. "standard"',
    )
    parser.add_argument(
        '--cluster-name',
        help='cluster name, e.g. "uswest1-devc" (defaults to local cluster)',
    )
    parser.add_argument(
        '--minutes',
        help=('check all the log files modified in the last --minutes minutes'),
        type=int,
    )
    parser.add_argument(
        '--start-time',
        help=('check all the log files modified after --start-time. '
              'Example format: --start-time "2015-11-26 11:00:00"'),
        type=str,
    )
    parser.add_argument(
        '--end-time',
        help=('check all the log files modified before --end-time. '
              'Example format: --start-time "2015-11-26 12:00:00"'),
        type=str,
    )
    parser.add_argument(
        '--batch-size',
        help=('Specify how many files to check in each run. '
              'Default: %(default)'),
        type=int,
        default=DEFAULT_BATCH_SIZE,
    )
    parser.add_argument(
        '--check-replicas',
        help='check data in replicas. Default: %(default) (leaders only)',
        action="store_true",
        default=False,
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help='print verbose execution information. Default: %(default)s',
        action="store_true",
    )
    return parser.parse_args()


def get_cluster(cluster_type, cluster_name):
    """Return the cluster configuration, given cluster type and name.
    Use the local cluster if cluster_name is not specified.

    :param cluster_type: the type of the cluster
    :type cluster_type: string
    :param cluster_name: the name of the cluster
    :type cluster_name: string
    :returns: the cluster
    :rtype: yelp_kafka.config.ClusterConfig
    """
    try:
        if cluster_name:
            return discovery.get_cluster_by_name(cluster_type, cluster_name)
        else:
            return discovery.get_local_cluster(cluster_type)
    except ConfigurationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)


def get_broker_list(cluster_config):
    """Returns a dictionary of brokers in the form {id: host}

    :param cluster_config: the configuration of the cluster
    :type cluster_config: yelp_kafka.config.ClusterConfig
    :returns: all the brokers in the cluster
    :rtype: map of (broker_id, host) pairs
    """
    with ZK(cluster_config) as zk:
        brokers = sorted(zk.get_brokers().items(), key=itemgetter(0))
        return [(int(id), data['host']) for id, data in brokers]


def find_files_cmd(data_path, minutes, start_time, end_time):
    """Find the log files depending on their modification time.

    :param data_path: the path to the Kafka data directory
    :type data_path: str
    :param minutes: check the files modified in the last N minutes
    :type minutes: int
    :param start_time: check the files modified after start_time
    :type start_time: str
    :param end_time: check the files modified before end_time
    :type end_time: str
    :returns: the find command
    :rtype: str
    """
    if minutes:
        command = FIND_MINUTES_COMMAND.format(
            data_path=data_path,
            minutes=minutes,
        )
    elif start_time:
        if end_time:
            command = FIND_RANGE_COMMAND.format(
                data_path=data_path,
                start_time=start_time,
                end_time=end_time,
            )
        else:
            command = FIND_START_COMMAND.format(
                data_path=data_path,
                start_time=start_time,
            )
    return command


def check_corrupted_files_cmd(java_home, files):
    """Check the file corruption of the specified files.

    :param java_home: the JAVA_HOME
    :type java_home: string
    :param files: list of files to be checked
    :type files: list of string
    """
    files_str = ",".join(files)
    check_command = CHECK_COMMAND.format(
        ionice=IONICE,
        java_home=java_home,
        files=files_str,
    )
    # One line per message can generate several MB/s of data
    # Use pre-filtering on the server side to reduce it
    command = "{check_command} | {reduce_output}".format(
        check_command=check_command,
        reduce_output=REDUCE_OUTPUT,
    )
    return command


def get_output_lines_from_command(host, command):
    """Execute a command on the specified host, returning a list of
    output lines.

    :param host: the host name
    :type host: str
    :param command: the command
    :type commmand: str
    """
    ssh = ssh_client(host)
    _, stdout, _ = ssh.exec_command(command)
    lines = stdout.read().splitlines()
    ssh.close()
    return lines


def find_files(brokers, minutes, start_time, end_time):
    """Find all the Kafka log files on the broker that have been modified
    in the speficied time range.

    :param brokers: the brokers
    :type brokers: list of (broker_id, host) pairs
    :param minutes: check the files modified in the last N minutes
    :type minutes: int
    :param start_time: check the files modified after start_time
    :type start_time: str
    :param end_time: check the files modified before end_time
    :type end_time: str
    :returns: the files
    :rtype: list of (broker, host, file_path) tuples
    """
    command = find_files_cmd(DATA_PATH, minutes, start_time, end_time)
    pool = Pool(len(brokers))
    result = pool.map(
        partial(get_output_lines_from_command, command=command),
        [host for broker, host in brokers])
    return [(broker, host, files)
            for (broker, host), files
            in zip(brokers, result)]


def parse_output(host, output):
    """Parse the output of the dump tool and prints warnings or error messages
    accordingly.

    :param host: the source
    :type host: str
    :param output: the output of the script on host
    :type output: list of str
    """
    current_file = None
    for line in output.readlines():
        file_name_search = FILE_PATH_REGEX.search(line)
        if file_name_search:
            current_file = file_name_search.group(1)
            continue
        if INVALID_MESSAGE_REGEX.match(line) or INVALID_BYTES_REGEX.match(line):
            print_line(host, current_file, line, "ERROR")
        elif VALID_MESSAGE_REGEX.match(line):
            continue
        else:
            print_line(host, current_file, line, "UNEXPECTED OUTPUT")


def print_line(host, path, line, line_type):
    """Print a dump tool line to stdout.

    :param host: the source host
    :type host: str
    :param path: the path to the file that is being analyzed
    :type path: str
    :param line: the line to be printed
    :type line: str
    :param line_type: a header for the line
    :type line_type: str
    """
    print(
        "{ltype} Host: {host}, File: {path}".format(
            ltype=line_type,
            host=host,
            path=path,
        )
    )
    print("{ltype} Output: {line}".format(ltype=line_type, line=line))


def check_files_on_host(host, files, batch_size):
    """Check the files on the host. Files are grouped together in groups
    of batch_size files. The dump class will be executed on each batch,
    sequentially.

    :param host: the host where the tool will be executed
    :type host: str
    :param files: the list of files to be analyzed
    :type files: list of str
    :param batch_size: the size of each batch
    :type batch_size: int
    """
    ssh = ssh_client(host)
    for i, batch in enumerate(chunks(files, batch_size)):
        command = check_corrupted_files_cmd(JAVA_HOME, batch)
        stdin, stdout, stderr = ssh.exec_command(command)
        print(
            "  {host}: file {n_file} of {total}".format(
                host=host,
                n_file=(i * DEFAULT_BATCH_SIZE),
                total=len(files),
            )
        )
        parse_output(host, stdout)
    ssh.close()


def get_partition_leaders(cluster_config):
    """Return the current leaders of all partitions. Partitions are
    returned as a "topic-partition" string.

    :param cluster_config: the cluster
    :type cluster_config: yelp_kafka.config.ClusterConfig
    :returns: leaders for partitions
    :rtype: map of ("topic-partition", broker_id) pairs
    """
    client = KafkaClient(cluster_config.broker_list)
    result = {}
    for topic, topic_data in client.topic_partitions.iteritems():
        for partition, p_data in topic_data.iteritems():
            topic_partition = topic + "-" + str(partition)
            result[topic_partition] = p_data.leader
    return result


def get_tp_from_file(file_path):
    """Return the name of the topic-partition given the path to the file.

    :param file_path: the path to the log file
    :type file_path: str
    :returns: the name of the topic-partition, ex. "ranger-0"
    :rtype: str
    """
    match = TP_FROM_FILE_REGEX.match(file_path)
    if not match:
        print("File path is not valid: " + file_path)
        sys.exit(1)
    return match.group(1)


def filter_leader_files(cluster_config, broker_files):
    """Given a list of broker files, filters out all the files that
    are in the replicas.

    :param cluster_config: the cluster
    :type cluster_config: yelp_kafka.config.ClusterConfig
    :param broker_files: the broker files
    :type broker_files: list of (b_id, host, [file_path, file_path ...]) tuples
    :returns: the filtered list
    :rtype: list of (broker_id, host, [file_path, file_path ...]) tuples
    """
    print("Filtering leaders")
    leader_of = get_partition_leaders(cluster_config)
    result = []
    for broker, host, files in broker_files:
        filtered = []
        for file_path in files:
            tp = get_tp_from_file(file_path)
            if tp not in leader_of or leader_of[tp] == broker:
                filtered.append(file_path)
        result.append((broker, host, filtered))
        print(
            "Broker: {broker}, leader of {l_count} over {f_count} files".format(
                broker=broker,
                l_count=len(filtered),
                f_count=len(files),
            )
        )
    return result


def check_cluster(
    cluster_config,
    check_replicas,
    batch_size,
    minutes,
    start_time,
    end_time,
):
    """Check the integrity of the Kafka log files in a cluster.

    :param check_replicas: also checks the replica files
    :type check_replicas: bool
    :param batch_size: the size of the batch
    :type batch_size: int
    :param minutes: check the files modified in the last N minutes
    :type minutes: int
    :param start_time: check the files modified after start_time
    :type start_time: str
    :param end_time: check the files modified before end_time
    :type end_time: str
    """
    brokers = get_broker_list(cluster_config)
    broker_files = find_files(brokers, minutes, start_time, end_time)
    if not check_replicas:  # remove replicas
        broker_files = filter_leader_files(cluster_config, broker_files)
    processes = []
    print("Starting {n} parallel processes".format(n=len(broker_files)))
    try:
        for broker, host, files in broker_files:
            print(
                "  Broker: {host}, {n} files to check".format(
                    host=host,
                    n=len(files)),
            )
            p = Process(
                target=check_files_on_host,
                args=(host, files, batch_size),
            )
            p.start()
            processes.append(p)
        print("Processes running:")
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        print("Terminating all processes")
        for process in processes:
            process.terminate()
            process.join()
        print("All processes terminated")
        sys.exit(1)


def validate_opts(opts):
    """Basic option validation. Returns True if the options are not valid,
    False otherwise.

    :param opts: the command line options
    :type opts: map
    :param brokers_num: the number of brokers
    """
    if not opts.minutes and not opts.start_time:
        print("Error: missing --minutes or --start-time")
        return True
    if opts.minutes and opts.start_time:
        print("Error: --minutes shouldn't be specified if --start-time is used")
        return True
    if opts.end_time and not opts.start_time:
        print("Error: --end-time can't be used without --start-time")
        return True
    if opts.minutes and opts.minutes <= 0:
        print("Error: --minutes must be > 0")
        return True
    if opts.start_time and not TIME_FORMAT_REGEX.match(opts.start_time):
        print("Error: --start-time format is not valid")
        return True
    if opts.end_time and not TIME_FORMAT_REGEX.match(opts.end_time):
        print("Error: --end-time format is not valid")
        return True
    if opts.batch_size <= 0:
        print("Error: --batch-size must be > 0")
        return True
    return False


def run_tool():
    opts = parse_opts()
    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)
    cluster_config = get_cluster(opts.cluster_type, opts.cluster_name)
    if validate_opts(opts):
        sys.exit(1)
    check_cluster(
        cluster_config,
        opts.check_replicas,
        opts.batch_size,
        opts.minutes,
        opts.start_time,
        opts.end_time,
    )
