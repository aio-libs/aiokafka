CHANGES
--------

0.1.3 (2016-10-18)
^^^^^^^^^^^^^^^^^^

* Fixed bug with infinite loop on heartbeats with autocommit=True. #44
* Bumped python-kafka to version 1.1.1
* Fixed docker test runner with multiple interfaces
* Minor documentation fixes


0.1.2 (2016-04-30)
^^^^^^^^^^^^^^^^^^

* Added Python3.5 usage example to docs
* Don't raise retriable exceptions in 3.5's async for iterator
* Fix Cancellation issue with producer's `send_and_wait` method


0.1.1 (2016-04-15)
^^^^^^^^^^^^^^^^^^

* Fix packaging issues. Removed unneded files from package.

0.1.0 (2016-04-15)
^^^^^^^^^^^^^^^^^^

Initial release

Added full support for Kafka 9.0. Older Kafka versions are not tested.
