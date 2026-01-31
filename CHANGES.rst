=========
Changelog
=========

0.14.0 (????-??-??)
===================

Bugfixes:

* Fix type annotation for `AIOKafkaAdminClient` (issue #1148)


0.13.0 (2026-01-02)
===================

Breaking changes:

* Resolve API versions at connection with brokers
  `api_version` parameter has been removed from the different clients (admin/consumer/producer)
  (pr #1136 by @vmaurin)

Improved Documentation:

* Fix incomplete documentation for `AIOKafkaConsumer.offset_for_times`
  (pr #1068 by @jzvandenoever)
* Fix Java Client API reference (pr #1069 by @emmanuel-ferdman)


Bugfixes:

* Make KafkaStorageError retriable after metadata refresh like in other
  implementations (pr #1115 by @omerhadari)
* Ensure the transaction coordinator is refreshed after broker fail‑over,
  so transactional producers resume once a new coordinator is elected.
  (pr #1135 by @vmaurin)
* Rename the RequestHeader version classes to match official version
  schemas (pr #1141 by @vmaurin)


Misc:

* Use SPDX license expression for project metadata.


0.12.0 (2024-10-26)
===================

New features:

* Build mac x86_64 wheels (pr #1029)
* Add support for Python 3.13, drop support for Python 3.8 due to end of life (pr #1061)
* Remove duplicate error logging during rebalance (pr #1025 by @y4n9squared)


Bugfixes:

* Quote username in SCRAM auth (pr #1043)


Improved Documentation:

* Fix building of readthedocs documentation (pr #1034)
* Fix typo in producer documentation (pr #1036 by @lgo)


0.11.0 (2024-06-30)
===================

New features:

* Implement DeleteRecords API (`KIP-204`_) (pr #969 by @vmaurin)

.. _KIP-204: https://cwiki.apache.org/confluence/display/KAFKA/KIP-204+%3A+Adding+records+deletion+operation+to+the+new+Admin+Client+API


Bugfixes:

* Fix serialization for batch (issue #886, pr #887 by @ydjin0602)
* Fix type annotation for `AIOKafkaAdminClient.create_partitions`
  (pr #978 by @alm0ra)
* Fix `NotControllerError` in `AIOKafkaAdminClient.create_topics` and other
  methods (issue #995)
* Fix unintended cancellation of fetcher task (issue #983, pr #1007 by @apmorton)


0.10.0 (2023-12-15)
===================

New features:

* Support static membership protocol, `KIP-345`_ (issue #680, pr #941 by
  @patkivikram and @joshuaherrera)

.. _KIP-345: https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances


Bugfixes:

* Fix extra dependencies (issue #952)


0.9.0 (2023-12-04)
==================

New features:

* Include `kafka-python` into `aiokafka`'s code base (issue #928 and others)
* Replace `python-snappy` and `zstandard` with `cramjam` (issue #930)
* PEP518 compliant `pyproject.toml`
* Python 3.12 support


Bugfixes:

* Fix type annotation for `ConsumerRecord` (pr #912 by @zschumacher)
* Improve send performance (issue #943)
* Fix `DescribeConfigsResponse_v1`


Improved Documentation:

* Fix `AbstractTokenProvider.token` example (pr #919 by @mtomilov)


0.8.1 (2023-05-31)
==================

New features:

* Drop support for Python 3.7 due to end of life (pr #893)


Bugfixes:

* Add SASL authentication support to `AIOKafkaAdminClient` (issue #889,
  pr #890 by @selevit)


Improved Documentation:

* Update `security_protocol` argument docstring (issue #883, pr #884 by
  @gabrielmbmb)
* Remove incorrect `await` for `AIOKafkaConsumer.highwater()` (pr #858 by
  @yi-jiayu)


0.8.0 (2022-11-21)
==================

New features:

* Add codec for ZStandard compression (KIP-110) (pr #801)
* Add basic admin client functionality (pr #811 started by @gabriel-tincu)
* Drop support for Python 3.6, add support and pre-built packages for Python
  3.10 (pr #841)


Bugfixes:

* Fix `KeyError` on solitary abort marker (issue #781, pr #782 by @pikulmar)
* Fix handling unsupported compression codec (issue #795)
* Handled other SASL mechanism in logging (issue #852, pr #861 by @mangin)


Improved Documentation:

* Fix documentation on how to install optional features (issue #645)
* Improve the rendering of the documentation (pr #722 by @multani)
* Fix `MyRebalancer` example in `docs/consumer.rst` (pr #731 by @aamalev)


0.7.2 (2021-09-02)
==================

Bugfixes:

* Fix `CancelledError` handling in sender (issue #710)
* Fix exception for weakref use after object deletion (issue #755)
* Fix consumer's `start()` method hanging after being idle for more than
  `max_poll_interval_ms` (issue #764)


Improved Documentation:

* Add `SASL_PLAINTEXT` and `SASL_SSL` to valid values of security protocol
  attribute (pr #768 by @pawelrubin)


0.7.1 (2021-06-04)
==================

Bugfixes:

* Allow group coordinator to close when all brokers are unavailable (issue #659
  and pr #660 by @dkilgore90)
* Exclude `.so` from source distribution to fix usage of sdist tarball
  (issue #681 and pr #684 by ods)
* Add `dataclasses` backport package to dependencies for Python 3.6
  (pr #690 by @ods)
* Fix initialization without running loop (issue #689 and pr #690 by @ods)
* Fix consumer fetcher for python3.9 (pr #672 by @dutradda)
* Make sure generation and member id are correct after (re)joining group.
  (issue #727 and pr #747 by @vangheem)


Deprecation:

* Add deprecation warning when loop argument to AIOKafkaConsumer and
  AIOKafkaProducer is passed.  It's scheduled for removal in 0.8.0 as a
  preparation step towards upcoming Python 3.10 (pr #699 by @ods)


Improved Documentation:

* Update docs and examples to not use deprecated practices like passing loop
  explicitly (pr #693 by @ods)
* Add docstring for Kafka header support in `Producer.send()` (issue #566 and
  pr #650 by @andreportela)


0.7.0 (2020-10-28)
==================

New features:

* Add support for Python 3.8 and 3.9. (issue #569, pr #669 and #676 by @ods)
* Drop support for Python 3.5. (pr #667 by @ods)
* Add `OAUTHBEARER` as a new `sasl_mechanism`. (issue #618 and pr #630 by @oulydna)


Bugfixes:

* Fix memory leak in kafka consumer when consumer is in idle state not consuming any message.
  (issue #628 and pr #629 by @iamsinghrajat)


0.6.0 (2020-05-15)
==================

New features:

* Add async context manager support for both Producer and Consumer. (pr #613 and #494 by @nimish)
* Upgrade to kafka-python version 2.0.0 and set it as non-strict
  parameter. (issue #590 by @yumendy and #558 by @originalgremlin)
* Make loop argument optional (issue #544)
* SCRAM-SHA-256 and SCRAM-SHA-512 support for SASL authentication (issue #571 and pr #588 by @SukiCZ)
* Added headers param to AIOKafkaProducer.send_and_wait (pr #553 by @megabotan)
* Add `consumer.last_poll_timestamp(partition)` which gives the ms timestamp of the last
  update of `highwater` and `lso`. (issue #523 and pr #526 by @aure-olli)
* Change all code base to async-await (pr #522)
* Minor: added PR and ISSUE templates to GitHub


Bugfixes:

* Ignore debug package generation on bdist_rpm command. (issue #599 by @gabriel-tincu)
* UnknownMemberId was raised to the user instead of retrying on auto commit. (issue #611)
* Fix issue with messages not being read after subscriptions change with group_id=None. (issue #536)
* Handle `RequestTimedOutError` in `coordinator._do_commit_offsets()` method to explicitly mark
  coordinator as dead. (issue #584 and pr #585 by @FedirAlifirenko)
* Added handling `asyncio.TimeoutError` on metadata request to broker and metadata update.
  (issue #576 and pr #577 by @MichalMazurek)
* Too many reqs on kafka not available (issue #496 by @lud4ik)
* Consumer.seek_to_committed now returns mapping of committed offsets (pr #531 by @ask)
* Message Accumulator: add_message being recursive eventually overflows (pr #530 by @ask)


Improved Documentation:

* Clarify auto_offset_reset usage. (pr 601 by @dargor)
* Fix spelling errors in comments and documentation using codespell (pr #567 by mauritsvdvijgh)
* Delete old benchmark file (issue #546 by @jeffwidman)
* Fix a few typos in docs (pr #573 and pr #563 by @ultrabug)
* Fix typos, spelling, grammar, etc (pr #545 and pr #547 by @jeffwidman)
* Fix typo in docs (pr #541 by @pablogamboa)
* Fix documentation for benchmark (pr #537 by @abhishekray07)
* Better logging for bad CRC (pr #529 by @ask)


0.5.2 (2019-03-10)
==================

Bugfixes:

* Fix ConnectionError breaking metadata sync background task (issue #517 and #512)
* Fix event_waiter reference before assignment (pr #504 by @romantolkachyov)
* Bump version of kafka-python


0.5.1 (2019-03-10)
==================

New features:

* Add SASL support with both SASL plain and SASL GGSAPI. Support also includes
  Broker v0.9.0, but you will need to explicitly pass ``api_version="0.9"``.
  (Big thanks to @cyrbil and @jsurloppe for working on this)
* Added support for max_poll_interval_ms and rebalance_timeout_ms settings (
  issue #67)
* Added pause/resume API for AIOKafkaConsumer. (issue #304)
* Added header support to both AIOKafkaConsumer and AIOKafkaProducer for
  brokers v0.11 and above. (issue #462)

Bugfixes:

* Made sure to not request metadata for all topics if broker version is passed
  explicitly and is 0.10 and above. (issue #440, thanks to @ulrikjohansson)
* Make sure heartbeat task will close if group is reset. (issue #372)


0.5.0 (2018-12-28)
==================

New features:

* Add full support for V2 format messages with a Cython extension. Those are
  used for Kafka >= 0.11.0.0
* Added support for transactional producing (issue #182)
* Added support for idempotent producing with `enable_idempotence` parameter
* Added support for `fetch_max_bytes` in AIOKafkaConsumer. This can help limit
  the amount of data transferred in a single roundtrip to broker, which is
  essential for consumers with large amount of partitions

Bugfixes:

* Fix issue with connections not propagating serialization errors
* Fix issue with `group=None` resetting offsets on every metadata update
  (issue #441)
* Fix issue with messages not delivered in order when Leader changes (issue
  #228)
* Fixed version parsing of `api_version` parameter. Before it ignored the
  parameter


0.4.3 (2018-11-01)
==================

Bugfix:

* Fixed memory issue introduced as a result of a bug in `asyncio.shield` and
  not cancelling coroutine after usage. (see issue #444 and #436)


0.4.2 (2018-09-12)
==================

Bugfix:

* Added error propagation from coordinator to main consumer. Before consumer
  just stopped with error logged. (issue #294)
* Fix manual partition assignment, broken in 0.4.0 (issue #394)
* Fixed RecursionError in MessageAccumulator.add_message (issue #409)
* Update kafka-python to latest 1.4.3 and added support for Python3.7
* Dropped support for Python3.3 and Python3.4

Infrastructure:

* Added Kafka 1.0.2 broker for CI test runner
* Refactored travis CI build pipeline

0.4.1 (2018-05-13)
==================

* Fix issue when offset commit error reports wrong partition in log (issue #353)
* Add ResourceWarning when Producer, Consumer or Connections are not closed
  properly (issue #295)
* Fix Subscription None in GroupCoordinator._do_group_rejoin (issue #306)


0.4.0 (2018-01-30)
==================

Major changes:

* Full refactor of the internals of AIOKafkaConsumer. Needed to avoid several
  race conditions in code (PR #286, fixes #258, #264 and #261)
* Rewrote Records parsing protocol to allow implementation of newer protocol
  versions later
* Added C extension for Records parsing protocol, boosting the speed of
  produce/consume routines significantly
* Added an experimental batch producer API for unique cases, where user wants
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
==================

* Added `AIOKafkaProducer.flush()` method. (PR #209 by @vineet-rh)
* Fixed a bug with uvloop involving `float("inf")` for timeout. (PR #210 by
   dmitry-moroz)
* Changed test runner to allow running tests on OSX. (PR #213 by @shargan)


0.3.0 (2017-08-17)
==================

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
==================

* Fixed retry problem in Producer, when buffer is not reset to 0 offset.
  Thanks to @ngavrysh for the fix in Tubular/aiokafka fork. (issue #184)
* Fixed how Producer handles retries on Leader node failure. It just did not
  work before... Thanks to @blugowski for the help in locating the problem.
  (issue #176, issue #173)
* Fixed degrade in v0.2.2 on Consumer with no group_id. (issue #166)


0.2.2 (2017-04-17)
==================

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
==================

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
==================

* Added SSL support. (PR #81 by Drizzt1991)
* Fixed UnknownTopicOrPartitionError error on first message for autocreated topic (PR #96 by fabregas)
* Fixed `next_record` recursion (PR #94 by fabregas)
* Fixed Heartbeat fail if no consumers (PR #92 by fabregas)
* Added docs addressing kafka-python and aiokafka differences (PR #70 by Drizzt1991)
* Added `max_poll_records` option for Consumer (PR #72 by Drizzt1991)
* Fix kafka-python typos in docs (PR #69 by jeffwidman)
* Topics and partitions are now randomized on each Fetch request (PR #66 by Drizzt1991)


0.1.4 (2016-11-07)
==================

* Bumped kafka-python version to 1.3.1 and Kafka to 0.10.1.0.
* Fixed auto version detection, to correctly handle 0.10.0.0 version
* Updated Fetch and Produce requests to use v2 with v0.10.0 message format on brokers.
  This allows a ``timestamp`` to be associated with messages.
* Changed lz4 compression framing, as it was changed due to KIP-57 in new message format.
* Minor refactorings

Big thanks to @fabregas for the hard work on this release (PR #60)


0.1.3 (2016-10-18)
==================

* Fixed bug with infinite loop on heartbeats with autocommit=True. #44
* Bumped kafka-python to version 1.1.1
* Fixed docker test runner with multiple interfaces
* Minor documentation fixes


0.1.2 (2016-04-30)
==================

* Added Python3.5 usage example to docs
* Don't raise retriable exceptions in 3.5's async for iterator
* Fix Cancellation issue with producer's `send_and_wait` method


0.1.1 (2016-04-15)
==================

* Fix packaging issues. Removed unneeded files from package.

0.1.0 (2016-04-15)
==================

Initial release

Added full support for Kafka 9.0. Older Kafka versions are not tested.
