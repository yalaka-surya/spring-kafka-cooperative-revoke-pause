# A cooperative partial revoke leaves the partitions a consumer *keeps* paused forever

## Summary

When a cooperative rebalance revokes every partition the in-flight poll drew its records from, the
partitions the member **keeps** are left paused in the Kafka client and are never resumed. They are
never fetched again for the lifetime of the consumer.

`ListenerConsumer.doPauseConsumerIfNecessary()` pauses the *entire* assignment while any
out-of-order ack is outstanding, and records that as one boolean, `consumerPaused`.
`onPartitionsRevoked` then drops the revoked partitions from `offsetsInThisBatch` and, if the map is
empty afterwards, sets `consumerPaused = false` — without calling `consumer.resume()`. Under the
cooperative protocol the retained partitions keep their `TopicPartitionState`, so they are still
paused in `SubscriptionState`; and because `doResumeConsumerIfNecessary()`'s resume sweep is gated
on `consumerPaused`, nothing ever resumes them.

Nothing surfaces. The container keeps polling, so heartbeats stay healthy, the member keeps its
partitions and no further rebalance is triggered. Nothing is logged above `DEBUG`. The only external
symptom is consumer lag on a subset of one member's partitions with a floor that never drains.

This is the variant of [#4687](https://github.com/spring-projects/spring-kafka/issues/4687) that the
reporter predicted but could not reproduce:

> Cooperative rebalancing still calls `onPartitionsRevoked` for partitions that genuinely move, so a
> rolling deploy or an autoscaler event can still revoke a partition that has in-flight acks. We have
> since observed exactly that in production with `cooperative-sticky` confirmed active.

Their `CooperativeAssignorTest` passes because its in-flight poll spans both partitions
(`max.poll.records` 8, four records on each), so whichever partition moves, `offsetsInThisBatch`
still holds an entry for the one that stays. The test below is the same scenario with the
in-flight batch confined to the partition that moves — the case `pendingOffsets.isEmpty()` was
written for.

**The fix for #4687 ([#4688](https://github.com/spring-projects/spring-kafka/pull/4688)) does not
address this.** That commit changed `ConsumerAcknowledgment` to ignore stale acknowledgments; it did
not touch `onPartitionsRevoked` or the resume path. No acknowledgment is involved in the sequence
below — the freeze is already in place before the in-flight work completes.

## Versions

Reproduced, unchanged, on every line:

| spring-kafka | kafka-clients | result |
|---|---|---|
| 3.3.16 | 3.8.1 | fails |
| 4.0.7 | 4.1.2 | fails |
| 4.1.1 (default) | 4.2.1 | fails |
| 4.2.0-M1 | 4.3.1 | fails |
| `main` @ `fff33914` — i.e. **after** #4688 | 4.3.1 | fails |

JDK 17. No Kotlin, no coroutines, no Spring context, no broker. The outcome is identical on five
consecutive runs of each.

## Reproducer

```bash
./gradlew test                                 # spring-kafka 4.1.1
./gradlew test -PspringKafkaVersion=3.3.16     # or any other affected version
```

To run it against a spring-kafka you built yourself — which is how the `main` row above was produced:

```bash
# in a spring-kafka checkout
./gradlew :spring-kafka:publishToMavenLocal
# here
./gradlew test -PspringKafkaVersion=4.2.0-SNAPSHOT
```

Four tests, differing only in what the container is holding when the revoke is processed:

| Test | State when the revoke is processed | Result |
|---|---|---|
| `retainedPartitionKeepsBeingConsumed` | in-flight batch came from the revoked partition only | **fails** |
| `retainedPartitionKeepsBeingConsumedAfterAnUnrelatedRevoke` | the acks emptied the map; the revoked partition never held a record | **fails** |
| `batchSpanningBothPartitionsSurvivesTheSameRevoke` | in-flight batch also covers the retained partition | passes |
| `plainListenerSurvivesTheSameRevoke` | no async replies, so no bookkeeping at all | passes |

The two controls hold everything else constant — same partitions, same records, same revoke — so the
only thing that distinguishes a failure is whether `offsetsInThisBatch` is empty at the moment the
revoke is processed.

**The trigger is broader than the headline case.** :4192 tests `pendingOffsets.isEmpty()` *after* the
removal, so it does not matter why the map is empty. The second failing test empties it with the
acknowledgements themselves and then revokes a partition that never held a record — same freeze. The
first variant is pure ordering; the second needs the acks to land after the previous iteration's
resume check and before the revoke, which in production is a race between the completing work and the
poll. Running both inside one poll task pins that interleaving so it is reproducible.

## The scenario

One container, concurrency 1 (the default), holding `cooperative.revoke-0` and
`cooperative.revoke-1`. Partition 0 is the one that moves to another member; partition 1 is retained.

1. One poll returns three records, all from **partition 0**.
2. The listener is asynchronous, so it returns without acknowledging: `offsetsInThisBatch` is
   `{cooperative.revoke-0=[0, 1, 2]}`.
3. On the next iteration `doPauseConsumerIfNecessary()` pauses **the whole assignment** — partition 1
   included, although it has nothing outstanding — and sets `consumerPaused = true`.
4. A cooperative rebalance revokes partition 0 and leaves partition 1 in place.
5. The in-flight work completes and acknowledges. (Immaterial: the freeze already happened.)
6. Three records are produced to **partition 1**, which this member still owns.

Step 6 is never delivered.

## Expected vs actual

Expected: the records produced to the retained partition are delivered.

Actual:

```
Cooperative partial revoke with async acks >
  the retained partition keeps being consumed when every in-flight record came from the revoked partition FAILED

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

The `consumer.paused()` and `ListenerConsumer` lines sit side by side: the client still
has `cooperative.revoke-1` paused, while the container believes `consumerPaused=false` and so will
never resume anything.

## Analysis

Line numbers are `spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java`
on `main` at `fff33914d4e450a33e17195e11a79937c3505605` (4.2.0-SNAPSHOT), i.e. after #4688.

**The pause is assignment-wide, and it is tracked as one boolean** (:2027-2039):

```java
if (!this.consumerPaused && (isPauseRequested() || this.pausedForAsyncAcks)
        || this.pauseForPending) {
    Collection<TopicPartition> assigned = getAssignedPartitions();
    if (!CollectionUtils.isEmpty(assigned)) {
        this.consumer.pause(assigned);          // <- every partition, not just the pending ones
        this.consumerPaused = true;
```

**The revoke clears the flag without releasing what it represents** (:4185-4196):

```java
synchronized (ListenerConsumer.this) {
    Map<TopicPartition, List<Long>> pendingOffsets = ListenerConsumer.this.offsetsInThisBatch;
    if (pendingOffsets != null) {
        partitions.forEach(tp -> {
            pendingOffsets.remove(tp);
            Objects.requireNonNull(ListenerConsumer.this.deferredOffsets).remove(tp);
        });
        if (pendingOffsets.isEmpty()) {
            ListenerConsumer.this.consumerPaused = false;   // <- no consumer.resume()
        }
    }
}
```

This is correct under an eager protocol, where `ConsumerCoordinator.onJoinPrepare` revokes everything
and calls `assignFromSubscribed(emptySet())` (kafka-clients 4.2.1 `ConsumerCoordinator`:827-834), so
every `TopicPartitionState` — and with it every pause flag — is discarded. It is not correct under
the cooperative protocol, where `onJoinComplete` revokes only `owned - assigned` (:423-443) and then
calls `assignFromSubscribed(assignedPartitions)` (:463), which **reuses** the existing state object
for a retained partition (`SubscriptionState`:319-325):

```java
for (TopicPartition tp : assignments) {
    TopicPartitionState state = this.assignment.stateValue(tp);
    if (state == null)
        state = new TopicPartitionState();      // <- only for partitions that are new
    assignedPartitionStates.put(tp, state);
}
```

So `cooperative.revoke-1` is still `paused` in the client.

**Nothing resumes it afterwards:**

- `doResumeConsumerIfNecessary()` (:2063-2076) clears `pausedForAsyncAcks`, but its
  `consumer.resume(consumer.paused())` sweep is guarded by `if (this.consumerPaused && ...)`, which
  the revoke already set to false.
- `repauseIfNeeded()` (:4246-4262) would set `consumerPaused` back to true, but it is guarded by
  `&& !partitions.isEmpty()` and a member that only *loses* partitions receives
  `onPartitionsAssigned(∅)` — `addedPartitions` is `assigned - owned`.
- `resumePartitionsIfNecessary()` only touches `pausedPartitions`, the user's per-partition pauses.

From then on `doPauseConsumerIfNecessary()` sees an empty map and does nothing, and `poll()` returns
nothing because the retained partition is not fetchable. Steady state, indefinitely.

`KafkaMessageListenerContainer` contains no reference to the rebalance protocol at all — outside the
KIP-932 share-group classes, `grep -ri cooperative` over spring-kafka's main sources returns nothing
— so it cannot distinguish the case where the pause is discarded for it from the case where it is
not. The warning text in `repauseIfNeeded` — *"Paused consumer resumed by Kafka due to rebalance"* —
encodes the eager-only model explicitly.

The two existing tests over this block, `ConcurrentMessageListenerContainerMockTests`'s
`pruneRevokedPartitionsFromPendingOutOfOrderCommitsLegacyAssignor` and
`...CoopAssignor`, cover the neighbouring cases and not this one. The legacy one revokes *all*
partitions, so nothing is retained; the cooperative one revokes `tp0` while assigning a new `tp2`,
which leaves `offsetsInThisBatch` holding `tp1` and hands `repauseIfNeeded` a non-empty set. Neither
reaches a cooperative revoke that empties the map with nothing added — which is the case here. Both
use a fully mocked `Consumer` whose `paused()` is always empty, so neither could observe a leaked
pause even if it hit the path.

## Why this is deterministic, and what is mocked

There is no race and no timing window. Every step runs on the consumer thread in a fixed order:
pause → poll → rebalance callbacks → resume check. The only probabilistic element in production is
external — whether the rebalance happens to move exactly the partitions the in-flight batch came
from — and the test removes it by driving the rebalance itself. Each step is submitted as a
`MockConsumer` poll task, so it runs *between* two container iterations rather than alongside them,
and the test waits on observable state before submitting the next one.

Only the broker round trip is mocked. The container, its `ConsumerRebalanceListener` and its ack path
are the real ones, and `MockConsumer` delegates to the real `SubscriptionState` — so the pause flag
surviving the partial revoke, which is the mechanism under test, is exercised rather than simulated.
`MockConsumer.rebalance` performs exactly the sequence `ConsumerCoordinator.onJoinComplete` performs
for a cooperative rebalance:

| `ConsumerCoordinator.onJoinComplete` (4.2.1) | `MockConsumer.rebalance` |
|---|---|
| `invokePartitionsRevoked(owned - assigned)` :443 | `onPartitionsRevoked(removed)` |
| `subscriptions.assignFromSubscribed(assigned)` :463 | `subscriptions.assignFromSubscribed(newAssignment)` |
| `invokePartitionsAssigned(assigned - owned)` :466 | `onPartitionsAssigned(added)` |

One deviation, and it is in the *conservative* direction: `MockConsumer.paused()` returns a `Set` it
maintains itself and never prunes on a rebalance, whereas `KafkaConsumer.paused()` returns
`SubscriptionState.pausedPartitions()`, which cannot contain an unassigned partition. The test
subclass scopes `paused()` to the current assignment to match the real client. Without that, the
*passing* control would break — `consumer.resume(consumer.paused())` would be handed a revoked
partition and throw.

Pass/fail is decided only by what is observable from outside the container: which records the
listener receives. Reflection appears once, to print the container's private state alongside a
failure.

## Configuration required

Nothing unusual, and nothing about acknowledgement:

| Setting | Value | |
|---|---|---|
| Listener return type | `CompletableFuture` / `Mono` / `suspend fun` | supported since 3.2 |
| `ackMode` | not set — `determineAckMode()` rewrites the default to `MANUAL` | default |
| `asyncAcks` | not set | default (`false`) |
| `enable.auto.commit` | not set — the container forces `false` | default |
| container `concurrency` | 1, holding more than one partition | default |
| `max.poll.records` | any | default |
| rebalance listener | none | default |
| `partition.assignment.strategy` | a cooperative assignor | **the only non-default setting** |

`CooperativeStickyAssignor` ships with kafka-clients, is in the default
`partition.assignment.strategy` list, and its javadoc recommends it (*"Users should prefer this
assignor for newer clusters"*). spring-kafka documents no incompatibility between it and async
replies or out-of-order commits, and asserts nothing about it at startup — although it does assert on
the combinations it knows are unsupported (`nack()` with `asyncReplies`, batch + `RECORD`, manual +
auto-commit).

The tests reach this path with a plain `AcknowledgingMessageListener` that reports
`AsyncRepliesAware.isAsyncReplies() == true`, which is exactly what `HandlerAdapter` reports for a
`@KafkaListener` returning `CompletableFuture`. It sets no ack mode and no `asyncAcks`.

## Why this is hard to detect

- The container logs pause/resume at `DEBUG` only, and the freeze itself logs nothing at all.
- No exception reaches a `CommonErrorHandler`; nothing reaches any error handler.
- The consumer keeps polling, so heartbeats stay healthy and no rebalance or eviction follows.
- Listener timings look normal, because the listener is simply never invoked.
- Lag on the *other* partitions of the same member keeps draining, so the member looks alive.

## Workarounds

- **An eager assignor.** `onJoinPrepare` discards every `TopicPartitionState`, so no pause survives.
  This is what #4687 recommends against, and it trades this failure for that one.
- **`registry.getListenerContainer(id).pause()` then `resume()`.** `isPauseRequested()` forces
  `consumerPaused = true` at :2033, and the next `doResumeConsumerIfNecessary()` sweeps
  `consumer.paused()`, which includes the stuck partitions.
- It also self-heals if a later rebalance *adds* a partition to this member: the fresh, unpaused
  state delivers records, `offsetsInThisBatch` refills, and the next pause/resume cycle sweeps
  `consumer.paused()`. A scale-up that leaves this member only losing partitions does not heal.
