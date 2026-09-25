# DLQ inclusive end timestamp: real Broker reproduction

Issue: https://github.com/apache/rocketmq-dashboard/issues/5103

The baseline exports 32 of 80 matching messages and reports neither truncation nor failed queues. The fixed provider exports all 80 and excludes the later message. All 80 messages are sent as a single batch and their identical Broker-assigned store timestamps are verified with a direct pull before invoking the real public `RocketMQDLQProvider.exportMessages` method.

Only instance-to-consumer resolution is supplied by the probe. Offset searches, pulls, message timestamps and export mapping are real. The Broker is disposable; this does not exercise a complete Studio/MySQL deployment, authentication, or old Broker versions.

## Recorded results

- `baseline-result.txt` and `replay-red-result.txt`: actual=32, failedQueues=0, truncated=false.
- `fixed-result.txt`: actual=80, failedQueues=0, truncated=false.
- `unit-green.log`: 44 provider tests passed, including multiple pull batches at the end timestamp, an empty window, and `Long.MAX_VALUE` overflow handling.
- `api-tests.log`: 44 service/controller/export-row tests passed.
- Both Maven test runs executed Checkstyle with zero violations.
- Full application tests and packaging were not run for this change. At the same baseline, the preceding Studio contribution reproduced 166 identical binary-license packaging errors before and after its change. No packaging or upstream CI success is claimed.

## Replay

Use JDK 21 for Studio and JDK 8 for the isolated Broker fixture. Maven 3.9.9 was used. See `environment.json` for exact source identities. The Broker checkout is commit `54a03d8b6a8f8a2b96f64aa301d434e23a7678f3` from `3219378872/rocketmq`, with its `rocketmq-apis` submodule at `3e60073c6dab3430feaec443824802e23ecda681`. That checkout contains a separate Proxy discovery fix, but this fixture starts only a NameServer and Broker, not a Proxy.

Prepare the Broker fixture from that checkout under JDK 8:

```sh
git submodule update --init
mvn -B -ntp -pl test -am -Dspotbugs.skip=true -DskipTests \
  test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/repro-classpath.txt
```

Skipping SpotBugs here only prepares the external fixture; this is not a Broker validation claim. Prepare the patched Studio checkout under JDK 21:

```sh
mvn -B -ntp -f server/pom.xml -DskipTests \
  test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/repro-classpath.txt
mvn -B -ntp -f server/pom.xml -Dtest=RocketMQDLQProviderTest test
mvn -B -ntp -f server/pom.xml \
  -Dtest=DLQServiceTest,DLQControllerTest,DLQMessageExcelRowTest test
```

Run from the evidence directory with JDK 21's `java` and `javac` on PATH:

```sh
python3 run_live.py --broker-checkout /path/to/rocketmq \
  --studio-checkout /path/to/rocketmq-dashboard --jdk8 /path/to/jdk8 \
  --baseline --label replay-red --expected 32
python3 run_live.py --broker-checkout /path/to/rocketmq \
  --studio-checkout /path/to/rocketmq-dashboard --jdk8 /path/to/jdk8 \
  --label fixed --expected 80
```

The baseline option compiles the original provider from immutable commit `a562601d0971001c1cd3b888a9f2664b6ef77811` into a temporary classpath overlay. The checkout must contain that commit. Each run creates and destroys its own Broker store and closes the clients. Shutdown interruption traces in Broker logs occur after the assertions and also appear on the baseline.

## Scope and review

The fix searches the lower offset boundary at `end + 1` and treats it as exclusive, matching the existing ordinary topic-message scan convention. `Long.MAX_VALUE` uses the queue's exclusive maximum offset instead of overflowing. Per-message inclusive filtering, scan caps and failure reporting remain in place. The API specification now states the inclusive storage-time interval.

All-state searches and related #4592, #4927, #4929 and #4718 were reviewed before publishing the Issue; no equivalent repair was found. The three production/verification/documentation files are reviewed by the primary agent. No subagent was used for this contribution.
