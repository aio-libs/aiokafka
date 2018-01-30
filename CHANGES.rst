CHANGES
--------

0.4.0 (2018-01-30)
^^^^^^^^^^^^^^^^^^

Major changes:

* Full refactor of the internals of AIOKafkaConsumer. Needed to avoid several
  race conditions in code (PR #286, fixes #258, #264 and #261)
* Rewrote Records parsing protocol to allow implementation of newer protocol
  versions later
* Added C extension for Records parsing protocol, boosting the speed of
  produce/consume routines significantly
* Added an experimental batch producer API for unique cases, where user want's
  to control batching himself (by @shargan)


Minor changes:

* Add `timestamp` field to produced message's metadata. This is needed to find
  LOG_APPEND_TIME configured timestamps.
* `Consumer.seek()` and similar API's now raise proper ``ValueError``'s on
  validation failure instead of ``AssertionError``.


Bug fixes:

* Fix ``connections_max_idle_ms`` option, as earlier it was only applied to
  bootstrap socket. (PR #299)
* Fix ``consumer.stop()`` side effect of logging an exception
  ConsumerStoppedError (issue #263)
* Problem with Producer not able to recover from broker failure (issue #267)
* Traceback containing duplicate entries due to exception sharing (PR #247
  by @Artimi)
* Concurrent record consumption rasing `InvalidStateError('Exception is not
  set.')` (PR #249 by @aerkert)
* Don't fail ``GroupCoordinator._on_join_prepare()`` if ``commit_offset()``
  throws exception (PR #230 by @shargan)
* Send session_timeout_ms to GroupCoordinator constructor (PR #229 by @shargan)

Big thanks to:

* @shargan for Producer speed enhancements and the batch produce API
  proposal/implementation.
* @vineet-rh and other contributors for constant feedback on Consumer
  problems, leading to the refactor mentioned above.


0.3.1 (2017-09-19)
^^^^^^^^^^^^^^^^^^

* Added `AIOKafkaProducer.flush()` method. (PR #209 by @vineet-rh)
* Fixed a bug with uvloop involving `float("inf")` for timeout. (PR #210 by 
   dmitry-moroz)
* Changed test runner to allow running tests on OSX. (PR #213 by @shargan)


0.3.0 (2017-08-17)
^^^^^^^^^^^^^^^^^^

* Moved all public structures and errors to `aiokafka` namespace. You will no
  longer need to import from `kafka` namespace.
* Changed ConsumerRebalanceListener to support either function or coroutine
  for `on_partitions_assigned` and `on_partitions_revoked` callbacks. (PR #190
  by @ask)
* Added support for `offsets_for_times`, `beginning_offsets`, `end_offsets`
  API's. (issue #164)
* Coordinator requests are now sent using a separate socket. Fixes slow commit
  issue. (issuer #137, issue #128)
* Added `seek_to_end`, `seek_to_beginning` API's. (issue #154)
* Updated documentation to provide more useful usage guide on both Consumer and
  Producer interface.

0.2.3 (2017-07-23)
^^^^^^^^^^^^^^^^^^

* Fixed retry problem in Producer, when buffer is not reset to 0 offset. 
  Thanks to @ngavrysh for the fix in Tubular/aiokafka fork. (issue #184)
* Fixed how Producer handles retries on Leader node failure. It just did not
  work before... Thanks to @blugowski for the help in locating the problem.
  (issue #176, issue #173)
* Fixed degrade in v0.2.2 on Consumer with no group_id. (issue #166)


0.2.2 (2017-04-17)
^^^^^^^^^^^^^^^^^^

* Reconnect after KafkaTimeoutException. (PR #149 by @Artimi)
* Fixed compacted topic handling. It could skip messages if those were
  compacted (issue #71)
* Fixed old issue with new topics not adding to subscription on pattern
  (issue #46)
* Another fix for Consumer race condition on JoinGroup. This forces Leader to
  wait for new metadata before assigning partitions. (issue #118)
* Changed metadata listener in Coordinator to avoid 2 rejoins in a rare
  condition (issue #108)
* `getmany` will not return 0 results until we hit timeout. (issue #117)

Big thanks to @Artimi for pointing out several of those issues.


0.2.1 (2017-02-19)
^^^^^^^^^^^^^^^^^^

* Add a check to wait topic autocreation in Consumer, instead of raising 
  UnknownTopicOrPartitionError (PR #92 by fabregas)
* Consumer now stops consumption after `consumer.stop()` call. Any new `get*` calls 
  will result in ConsumerStoppedError (PR #81)
* Added `exclude_internal_topics` option for Consumer (PR #111)
* Better support for pattern subscription when used with `group_id` (part of PR #111)
* Fix for Consumer `subscribe` and JoinGroup race condition (issue #88). Coordinator will now notice subscription changes during rebalance and will join group again. (PR #106)
* Changed logging messages according to KAFKA-3318. Now INFO level should be less messy and more informative. (PR #110)
* Add support for connections_max_idle_ms config (PR #113)


0.2.0 (2016-12-18)
^^^^^^^^^^^^^^^^^^

* Added SSL support. (PR #81 by Drizzt1991)
* Fixed UnknownTopicOrPartitionError error on first message for autocreated topic (PR #96 by fabregas)
* Fixed `next_record` recursion (PR #94 by fabregas)
* Fixed Heartbeat fail if no consumers (PR #92 by fabregas)
* Added docs addressing kafka-python and aiokafka differences (PR #70 by Drizzt1991)
* Added `max_poll_records` option for Consumer (PR #72 by Drizzt1991)
* Fix kafka-python typos in docs (PR #69 by jeffwidman)
* Topics and partitions are now randomized on each Fetch request (PR #66 by Drizzt1991)


0.1.4 (2016-11-07)
^^^^^^^^^^^^^^^^^^

* Bumped kafka-python version to 1.3.1 and Kafka to 0.10.1.0.
* Fixed auto version detection, to correctly handle 0.10.0.0 version
* Updated Fetch and Produce requests to use v2 with v0.10.0 message format on brokers.
  This allows a ``timestamp`` to be associated with messages.
* Changed lz4 compression framing, as it was changed due to KIP-57 in new message format.
* Minor refactorings

Big thanks to @fabregas for the hard work on this release (PR #60)


0.1.3 (2016-10-18)
^^^^^^^^^^^^^^^^^^

* Fixed bug with infinite loop on heartbeats with autocommit=True. #44
* Bumped kafka-python to version 1.1.1
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
