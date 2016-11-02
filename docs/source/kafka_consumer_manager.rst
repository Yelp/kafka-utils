Consumer Manager
****************

This kafka tool provides the ability to view and manipulate consumer offsets
for a specific consumer group. For a given cluster, this tool provides us with
the following functionalities:

* **Manipulating consumer-groups**: Listing consumer-groups subscribed to the
  cluster. Copying, deleting and renaming of the group.

* **Manipulating offsets**: For a given consumer-group, fetching current offsets,
  low and high watermarks for topics and partitions subscribed to the group.
  Setting, advancing, rewinding, saving and restoring of current-offsets.

* **Manipulating topics**: For a given consumer-group and cluster, listing and
  unsubscribing topics.

* **Offset storage choice**: Supports Kafka 0.8.2 and 0.9.0, using offsets
  stored in either Zookeeper or Kafka. Version 0 and 2 of the Kafka Protocol
  are supported for committing offsets.

Subcommands
===========

* copy_group
* delete_group
* list_groups
* list_topics
* offset_advance
* offset_get
* offset_restore
* offset_rewind
* offset_save
* offset_set
* rename_group
* unsubscribe_topics


Listing consumer groups
=======================

The :code:`list_groups` command shows all of the consumer groups that exist in
the cluster.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type=test list_groups
     Consumer Groups:
   	 group1
   	 group2
   	 group3

If :code:`list_groups` is called with the :code:`--storage` option, then the groups will
only be fetched from Zookeeper or Kafka.


Listing topics
==============

For information about the topics subscribed by a consumer group, the
`list_topics` subcommand can be used.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type=test list_topics group3
     Consumer Group ID: group3
    	 Topic: topic_foo
    		Partitions: [0, 1, 2, 3, 4, 5]
    	 Topic: topic_bar
    		Partitions: [0, 1, 2]


Getting consumer offsets
========================

The :code:`offset_get` subcommand gets information about a specific consumer group.

The most basic usage is to call :code:`offset_get` with a consumer group id.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type test --cluster-name my_cluster offset_get my_group
     Cluster name: my_cluster, consumer group: my_group
     Topic Name: topic1
    	Partition ID: 0
    		High Watermark: 787656
    		Low Watermark: 787089
    		Current Offset: 787645

The offsets for all topics in the consumer group will be shown by default.
A single topic can be specified using the :code:`--topic` option. If a topic is
specified, then a list of partitions can also be specified using the
:code:`--partitions` option.

By default, the offsets will be fetched from both Zookeeper and Kafka's
internal offset storage. A specific offset storage location can be speficied
using the :code:`--storage` option.


Manipulating consumer offsets
=============================

The offsets for a consumer group can also be saved into a json file.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type test --cluster-name my_cluster offset_save my_group my_offsets.json
     Cluster name: my_cluster, consumer group: my_group
     Consumer offset data saved in json-file my_offsets.json

The save offsets file can then be used to restore the consumer group.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type test --cluster-name my_cluster offset_restore my_offsets.json
     Restored to new offsets {u'topic1': {0: 425447}}

The offsets can also be set directly using the :code:`offset_set` command. This
command takes a group id, and a set of topics, partitions, and offsets.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type test --cluster-name my_cluster offset_set my_group topic1.0.38531

There is also an :code:`offset_advance` command, which will advance the current offset
to the same value as the high watermark of a topic, and an :code:`offset_rewind`
command, which will rewind to the low watermark.

If the offset needs to be modified for a consumer group does not already
exist, then the :code:`--force` option can be used. This option can be used with
:code:`offset_set`, :code:`offset_rewind`, and :code:`offset_advance`.


Copying or renaming consumer group
==================================

Consumer groups can have metadata copied into a new group using the
:code:`rename_group` subcommand.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type=test copy_group my_group1 my_group2


They can also be renamed using :code:`rename_group`.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type=test rename_group my_group1 my_group2

When the group is copied, if a topic is specified using the :code:`--topic` option,
then only the offsets for that topic will be copied. If a topic is specified,
then a set of partitions of that topic can also be specified using the
:code:`--partitions` option.

Deleting or unsubscribing consumer groups
=========================================

A consumer group can be deleted using the :code:`delete_group` subcommand.

.. code-block:: bash

   $ kafka-consumer-manager --cluster-type=test delete_group my_group

A consumer group be unsubscribed from topics using the :code:`unsubscribe_topics`
subcommand. If a single topic is specified using the :code:`--topic` option, then
the group will be unsubscribed from only that topic.
