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
