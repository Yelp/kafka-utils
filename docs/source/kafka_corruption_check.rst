Corruption Check
****************

The kafka-corruption-check script performs a check on the log files stored on
the Kafka brokers. This tool finds all the log files modified in the specified
time range and runs
`DumpLogSegments <https://github.com/apache/kafka/blob/0.9.0/core/src/main/scala/kafka/tools/DumpLogSegments.scala>`_
on them. The output is collected and filtered. All the output lines that
are related to corrupted messages will be reported to the user.

Parameters
==========

The parameters specific for kafka-corruption-check are:

* ``--minutes N``: check the log files modified in the last ``N`` minutes.
* ``--start-time START_TIME``: check the log files modified after
  ``START_TIME``.
* ``--end-time END_TIME``: the number of seconds between each check.
* ``--data-path``: the path to the data files on the Kafka broker.
* ``--java-home``: the JAVA_HOME on the Kafka broker.
* ``--batch-size BATCH_SIZE``: the number of files that will be checked
  in parallel on each broker..
* ``--check-replicas``: if set it will also check the data on replicas.
  Default: false.
* ``--verbose``: enable verbose output.

Examples
========

Check all the files (leaders only) in the generic dev cluster, whose
topic name starts with 'services' and that were modified in the last 30
minutes:

.. code-block:: bash

   $ kafka-corruption-check --cluster-type generic --cluster-name dev --data-path /var/kafka-logs --minutes 30 --prefix "services"
   Filtering leaders
   Broker: 0, leader of 9 over 13 files
   Broker: 1, leader of 4 over 11 files
   Starting 2 parallel processes
     Broker: broker0.example.org, 9 files to check
     Broker: broker1.example.org, 4 files to check
   Processes running:
     broker0.example.org: file 0 of 9
     broker0.example.org: file 5 of 9
   ERROR Host: broker0.example.org: /var/kafka-logs/test_topic-0/00000000000000003363.log
   ERROR Output: offset: 3371 position: 247 isvalid: false payloadsize: 22 magic: 0 compresscodec: NoCompressionCodec crc: 2230473982
     broker1.example.org: file 0 of 4

In this example, one corrupted file was found in broker 0.

Check all the files modified after the specified date, in both leaders and replicas:

.. code-block:: bash

  $ kafka-corruption-check --cluster-type generic --cluster-name dev --start-time "2015-11-26 11:00:00" --check-replicas

Check all the files that were modified in the specified range and that do not start with 'services':

.. code-block:: bash

  $ kafka-corruption-check --cluster-type generic --cluster-name dev --start-time "2015-11-26 11:00:00" --end-time "2015-11-26 12:00:00" --prefix "!services"

For more information refer to ``--help``.

Even though this too executes the log check with a low ionice priority, it can
slow down the cluster given the high number of io operations required.
