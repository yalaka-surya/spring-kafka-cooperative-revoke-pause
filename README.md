# A cooperative partial revoke can leave the partitions a consumer *keeps* unconsumed

> **This repository was generated entirely by an AI agent (Claude).** The test, the build and this
> README are AI output. It has been read before publishing, but not written by a human.
>
> It deliberately contains **no analysis of the cause and no proposed fix**. Earlier drafts had both
> and they were removed: the point of this repository is to hand spring-kafka's maintainers a
> reproducer, not to steer their diagnosis. Nothing here classifies the behaviour as a defect.

## What it shows

With an async listener (`CompletableFuture` / `Mono` / a Kotlin `suspend fun`) and a cooperative
assignor, a rebalance that revokes one partition can leave the partitions the consumer keeps
permanently unconsumed. Records on them are never delivered again and never committed, for the
lifetime of the consumer.

Nothing surfaces. The container keeps polling, so heartbeats stay healthy and no further rebalance is
triggered. Nothing is logged above `DEBUG`. No exception reaches a `CommonErrorHandler`. Lag on the
member's *other* partitions keeps draining, so the member looks alive. The only external symptom is
consumer lag on a subset of one member's partitions with a floor that never drains.

Related: [#4687](https://github.com/spring-projects/spring-kafka/issues/4687), whose reporter noted
that cooperative rebalancing still calls `onPartitionsRevoked` for partitions that genuinely move.
This still reproduces on `main` with [#4688](https://github.com/spring-projects/spring-kafka/pull/4688)
applied.

## Versions

| spring-kafka | kafka-clients | result |
|---|---|---|
| 3.3.16 | 3.8.1 | fails |
| 4.0.7 | 4.1.2 | fails |
| 4.1.1 (default) | 4.2.1 | fails |
| 4.2.0-M1 | 4.3.1 | fails |
| `main` @ `fff33914` — i.e. with #4688 | 4.3.1 | fails |

JDK 17. The outcome is identical on five consecutive runs of each.

## Running it

```bash
./gradlew test                                 # spring-kafka 4.1.1
./gradlew test -PspringKafkaVersion=3.3.16     # or any other version above
```

Against a spring-kafka you built yourself, which is how the `main` row was produced:

```bash
# in a spring-kafka checkout
./gradlew :spring-kafka:publishToMavenLocal
# here
./gradlew test -PspringKafkaVersion=4.2.0-SNAPSHOT
```

## The four tests

| Test | Situation | Result |
|---|---|---|
| `retainedPartitionKeepsBeingConsumed` | the in-flight batch came from the revoked partition only | **fails** |
| `retainedPartitionKeepsBeingConsumedAfterAnUnrelatedRevoke` | the in-flight work has acknowledged, and the revoked partition never held a record | **fails** |
| `batchSpanningBothPartitionsSurvivesTheSameRevoke` | the in-flight batch also covers the retained partition | passes |
| `plainListenerSurvivesTheSameRevoke` | the listener is not async | passes |

The two passing controls hold everything else constant — same partitions, same records, same revoke —
so they narrow what distinguishes a failure. The second failing case is worth noting separately: the
partition that moves never held a record, so the in-flight batch does not have to come from the
revoked partitions for this to happen.

A failure prints the container's state alongside the assertion:

```
org.opentest4j.AssertionFailedError: [offsets delivered from the retained partition cooperative.revoke-1
  consumer.assignment() = [cooperative.revoke-1]
  consumer.paused()     = [cooperative.revoke-1]
  delivered             = [cooperative.revoke-0@0, cooperative.revoke-0@1, cooperative.revoke-0@2]
  ListenerConsumer      = offsetsInThisBatch={} pausedForAsyncAcks=false consumerPaused=false]
Expecting actual:
  []
to contain exactly (and in same order):
  [0L, 1L, 2L]
```

## Determinism, and what is mocked

No broker, no second consumer, no Spring context, no Kotlin. The container, its
`ConsumerRebalanceListener` and its ack path are the real ones; only the broker round trip is
replaced, by `MockConsumer`, which delegates to the real `SubscriptionState`. `MockConsumer.rebalance`
performs the same sequence `ConsumerCoordinator.onJoinComplete` performs for a cooperative rebalance:

| `ConsumerCoordinator.onJoinComplete` | `MockConsumer.rebalance` |
|---|---|
| `invokePartitionsRevoked(owned - assigned)` | `onPartitionsRevoked(removed)` |
| `subscriptions.assignFromSubscribed(assigned)` | `subscriptions.assignFromSubscribed(newAssignment)` |
| `invokePartitionsAssigned(assigned - owned)` | `onPartitionsAssigned(added)` |

Every step runs on the consumer thread in a fixed order. Each step is submitted as a `MockConsumer`
poll task so it lands between two container iterations, and the test waits on observable state before
submitting the next.

One deviation from a real client, and it is in the conservative direction: `MockConsumer.paused()`
returns a `Set` it maintains itself and never prunes on a rebalance, whereas `KafkaConsumer.paused()`
returns `SubscriptionState.pausedPartitions()`, which cannot contain an unassigned partition. The
test subclass scopes `paused()` to the current assignment to match the real client. Without that, one
of the *passing* controls would break on a call that a real consumer would accept.

Pass/fail depends only on what is observable from outside the container: which records the listener
receives. Reflection is used in one place, to print the container's internal state alongside a
failure.

## Configuration

Nothing unusual, and nothing set about acknowledgement:

| Setting | Value | |
|---|---|---|
| Listener return type | `CompletableFuture` / `Mono` / `suspend fun` | supported since 3.2 |
| `ackMode` | not set | default |
| `asyncAcks` | not set | default (`false`) |
| `enable.auto.commit` | not set | default |
| container `concurrency` | 1, holding more than one partition | default |
| `max.poll.records` | any | default |
| rebalance listener | none | default |
| `partition.assignment.strategy` | a cooperative assignor | **the only non-default setting** |

The tests use a plain `AcknowledgingMessageListener` that reports
`AsyncRepliesAware.isAsyncReplies() == true`, which is what `HandlerAdapter` reports for a
`@KafkaListener` returning `CompletableFuture`. This keeps the reproducer free of a Spring context.

## Clearing a stuck consumer

Calling `pause()` and then `resume()` on the container, e.g. through
`KafkaListenerEndpointRegistry`, gets it consuming again without a restart. It also recovers on its
own if a later rebalance *adds* a partition to the member; a rebalance that only takes partitions
away does not recover it.
