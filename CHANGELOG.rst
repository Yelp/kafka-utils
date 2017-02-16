0.6.4 (February 15, 2017)
------------------------
* Upgrade kafka-python in use to 1.3.2
* Use new KafkaConsumer for KafkaGroupReader

0.6.3 (January 26, 2017)
------------------------
* Fix KafkaGroupreader when reading consumer group with partition zero.

0.6.2 (January 25, 2017)
------------------------
* Add storage option for a few kafka_consumer_manager subcommands
* Change default offset storage from zookeeper to kafka
* Autodetecting the number of partitions for the __commit_offsets topic

0.6.1 (December 15, 2016)
-------------------------
* Fix integration tests

0.6.0 (December 15, 2016)
-------------------------
* Refactor kafka-cluster-manager to support multiple balancer classes and metrics
* Add PartitionMeasurer class and --partition-measurer option for providing user partition metrics
* Add --genetic-balancer option to kafka-cluster-manager to make use of the genetic balancer
* Change kafka-cluster-manager stats command output to include user partition metrics
* Add --show-stats option to kafka-cluster-manager rebalance

0.5.7 (December 12, 2016)
------------------------
* Fetch group topics only from a single __consumer_offsets partition

0.5.6 (December 8, 2016)
------------------------
* Add offline partitions check for kafka-check

0.5.5 (November 15, 2016)
-------------------------
* Fix set_replication_factor command plan generation

0.5.4 (November 15, 2016)
-------------------------
* Fix offset_get when the group name is stored only in kafka
* Add offset_set retry when writing offsets to kafka

0.5.3 (November 4, 2016)
------------------------
* Fix a rebalance bug that would not generate a convergent assignment
* Check for pending asssignment before fetching the cluster topology
* Docs fixes

0.5.2 (November 1, 2016)
------------------------
* Add short options from cluster-type and cluster-name

0.5.1 (October 14, 2016)
------------------------
* Add option to see offset-distance for a consumer-group

0.5.0 (September 23, 2016)
--------------------------
* Add command set_replication_factor command
* Fix kafka-cluster-manager error on empty clusters

0.4.2 (September 2, 2016)
-------------------------
* Fix bug in cluster rebalance while updating sibling_distance

0.4.1 (September 1, 2016)
-------------------------
* Fix bug in cluster rebalance when replication group is None

0.4.0 (August 19, 2016)
-----------------------
* Add get topic watermark command
* Fix offset get json output

0.3.3 (July 29, 2016)
---------------------
* Fix bug in decommissioning of failed brokers

0.3.2 (July 14, 2016)
---------------------
* Make min_isr and under replicated partitions check much faster

0.3.1 (July 5, 2016)
---------------------
* Use error field from metadata response in under replicated partition check
* Fix small typo in cluster manager logging

0.3.0 (July 1, 2016)
---------------------
* Refactor under replicated partition check to use metadata request
* Add minimum replica number parameter to under replicated check
* Fix cluster manager logging

0.2.1 (June 21, 2016)
---------------------
* Add verbose option to kafka-check

0.2.0 (June 15, 2016)
----------------------
* Add under replicated partition check
* Add log segment corruption check
* Fix decommission command bug that caused decommission to fail in some cases
* Fix config when HOME env variable is not defined

0.1.2 (June 8, 2016)
----------------------
* Fix bug for no available under-loaded brokers

0.1.1 (May 17, 2016)
----------------------

* Fix group-parser local import

0.1.0 (May 17, 2016)
----------------------

* Initial open-source release
