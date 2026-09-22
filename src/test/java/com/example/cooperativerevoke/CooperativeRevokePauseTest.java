package com.example.cooperativerevoke;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.adapter.AsyncRepliesAware;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A cooperative rebalance that revokes every partition the in-flight poll drew from leaves the
 * partitions this member <em>keeps</em> paused forever.
 *
 * <p>The sequence, all of it on the consumer thread and all of it in
 * {@code KafkaMessageListenerContainer.ListenerConsumer}:
 *
 * <ol>
 * <li>An async listener (one returning {@code CompletableFuture}, {@code Mono} or a Kotlin
 *     {@code suspend fun}) makes {@code asyncReplies} true, which forces {@code AckMode.MANUAL}
 *     and creates {@code offsetsInThisBatch} / {@code deferredOffsets}.</li>
 * <li>While a record is unacknowledged, {@code doPauseConsumerIfNecessary()} pauses the
 *     <em>entire</em> assignment and records that as one boolean, {@code consumerPaused}. A
 *     pending offset on {@link #MOVED} therefore also pauses {@link #RETAINED}.</li>
 * <li>{@code onPartitionsRevoked} drops the revoked partitions from {@code offsetsInThisBatch},
 *     and if the map is then empty sets {@code consumerPaused = false} — without calling
 *     {@code consumer.resume()}.</li>
 * <li>Under the cooperative protocol {@code SubscriptionState.assignFromSubscribed} reuses the
 *     existing {@code TopicPartitionState} for retained partitions, so {@link #RETAINED} is still
 *     paused in the client. (Under an eager protocol every state object is discarded first, which
 *     is why this variant needs a cooperative assignor.)</li>
 * <li>{@code doResumeConsumerIfNecessary()} clears {@code pausedForAsyncAcks}, but its
 *     {@code consumer.resume(consumer.paused())} sweep is gated on {@code consumerPaused}, which
 *     step 3 already set to false. Nothing ever resumes {@link #RETAINED}.</li>
 * </ol>
 *
 * <p>The container keeps polling and keeps heartbeating, so it stays a healthy group member and no
 * further rebalance is triggered. Nothing is logged above DEBUG. The only external symptom is
 * consumer lag on a subset of one member's partitions that never drains.
 *
 * <p><b>Why this is deterministic.</b> There is no race and no timing window: every step above runs
 * on the consumer thread in a fixed order, and the only external variable is whether the rebalance
 * happens to revoke exactly the partitions the in-flight batch came from. The tests fix that by
 * driving the rebalance themselves, so the outcome is decided by ordering alone.
 *
 * <p><b>What is mocked.</b> Only the broker round trip. The container, its rebalance listener and
 * its ack path are the real ones, and {@link MockConsumer} delegates to the real
 * {@code SubscriptionState} — so step 4, the mechanism under test, is exercised rather than
 * simulated. {@code MockConsumer.rebalance} performs exactly the sequence
 * {@code ConsumerCoordinator.onJoinComplete} performs for a cooperative rebalance: invoke
 * {@code onPartitionsRevoked(owned - assigned)}, then {@code assignFromSubscribed(assigned)}, then
 * {@code onPartitionsAssigned(assigned - owned)}.
 *
 * <p>Pass/fail is decided only by what is observable from outside the container: which records the
 * listener receives. Reflection appears once, in {@link Harness#diagnostics()}, to print the
 * container's private state alongside a failure.
 */
@DisplayName("Cooperative partial revoke with async acks")
class CooperativeRevokePauseTest {

	private static final String TOPIC = "cooperative.revoke";

	private static final String GROUP = "cooperative-revoke";

	/** The partition the rebalance hands to another member. */
	private static final TopicPartition MOVED = new TopicPartition(TOPIC, 0);

	/** The partition this member keeps across the rebalance. This is the one that freezes. */
	private static final TopicPartition RETAINED = new TopicPartition(TOPIC, 1);

	private static final int BATCH = 1;

	/** max.poll.records for both the container's config and the mock broker. */
	private static final int MAX_POLL_RECORDS = 1;

	private static final Duration SETTLE = Duration.ofSeconds(15);

	@Test
	@DisplayName("the retained partition keeps being consumed when every in-flight record came from the revoked partition")
	void retainedPartitionKeepsBeingConsumed() {
		try (Harness harness = Harness.started(Harness.ASYNC_LISTENER)) {
			// One poll, drawing records from the partition that is about to move, and only from it.
			harness.assignBothAndDeliver(MOVED);
			harness.awaitDeliveries(BATCH);

			// Nothing has been acknowledged, so the container pauses the whole assignment -
			// including RETAINED, which has nothing outstanding of its own.
			harness.awaitPaused(MOVED, RETAINED);

			// The cooperative partial revoke: MOVED goes to another member, RETAINED stays here.
			// offsetsInThisBatch loses its only key, so consumerPaused is set back to false with
			// RETAINED still paused in the client.
			harness.rebalanceTo(RETAINED);

			// The in-flight work finishes and acknowledges. Its bookkeeping was already discarded
			// by the revoke, so this changes nothing either way.
			harness.acknowledgeEverythingInFlight();

			// New records arrive on the partition this member still owns.
			harness.produce(RETAINED, BATCH);

			await().atMost(SETTLE).untilAsserted(() ->
					assertThat(harness.deliveredOffsets(RETAINED))
							.describedAs("offsets delivered from the retained partition %s%n%s",
									RETAINED, harness.diagnostics())
							.containsExactlyElementsOf(offsets(BATCH)));
		}
	}

	@Test
	@DisplayName("the retained partition keeps being consumed when a revoke of an unrelated partition lands while the consumer is paused")
	void retainedPartitionKeepsBeingConsumedAfterAnUnrelatedRevoke() {
		try (Harness harness = Harness.started(Harness.ASYNC_LISTENER)) {
			// This time the in-flight batch belongs to the partition that is KEPT, and the
			// partition that moves never had a record in it. The trigger is not "the batch came
			// from the revoked partitions" but the weaker "offsetsInThisBatch is empty when the
			// revoke is processed", which the acks satisfy on their own.
			harness.assignBothAndDeliver(RETAINED);
			harness.awaitDeliveries(BATCH);
			harness.awaitPaused(MOVED, RETAINED);

			// The acks empty the map, and the revoke of MOVED lands before the container reaches
			// its resume check. In production this is a race between the ack thread and the poll;
			// here both happen inside one poll, which pins the interleaving.
			harness.acknowledgeThenRebalanceTo(RETAINED);

			harness.produce(RETAINED, BATCH);

			await().atMost(SETTLE).untilAsserted(() ->
					assertThat(harness.deliveredOffsets(RETAINED))
							.describedAs("offsets delivered from the retained partition %s%n%s",
									RETAINED, harness.diagnostics())
							.containsExactlyElementsOf(offsets(2 * BATCH)));
		}
	}

	@Test
	@DisplayName("max.poll.records=1 does not avoid it: one unacknowledged record is enough")
	void retainedPartitionKeepsBeingConsumedWithMaxPollRecordsOfOne() {
		try (Harness harness = Harness.started(Harness.ASYNC_LISTENER, 1)) {
			// One record per poll means the in-flight batch can only ever come from a single
			// partition, so the situation the first test sets up deliberately is the only one this
			// setting can produce. Throttling the poll does not narrow the window - it removes the
			// batch spread that makes the control below pass.
			harness.assignBothAndDeliver(MOVED);
			harness.awaitDeliveries(1);
			assertThat(harness.deliveredOffsets(MOVED))
					.describedAs("the container pauses the whole assignment after one unacknowledged record")
					.containsExactly(0L);

			harness.awaitPaused(MOVED, RETAINED);
			harness.rebalanceTo(RETAINED);
			harness.acknowledgeEverythingInFlight();
			harness.produce(RETAINED, BATCH);

			// Only the first record is expected: with max.poll.records=1 and nothing acknowledging,
			// the container pauses again immediately after it. Delivering it at all is what the
			// freeze prevents.
			await().atMost(SETTLE).untilAsserted(() ->
					assertThat(harness.deliveredOffsets(RETAINED))
							.describedAs("offsets delivered from the retained partition %s%n%s",
									RETAINED, harness.diagnostics())
							.containsExactly(0L));
		}
	}

	@Test
	@DisplayName("control: the same revoke is harmless when the in-flight batch also covers the retained partition")
	void batchSpanningBothPartitionsSurvivesTheSameRevoke() {
		try (Harness harness = Harness.started(Harness.ASYNC_LISTENER)) {
			// Identical to the failing test except for this line: the poll draws records from both
			// partitions, so revoking MOVED leaves offsetsInThisBatch non-empty, consumerPaused
			// stays true, and the resume sweep runs once the remaining acks clear.
			harness.assignBothAndDeliver(MOVED, RETAINED);
			harness.awaitDeliveries(2 * BATCH);
			harness.awaitPaused(MOVED, RETAINED);

			harness.rebalanceTo(RETAINED);
			harness.acknowledgeEverythingInFlight();
			harness.produce(RETAINED, BATCH);

			await().atMost(SETTLE).untilAsserted(() ->
					assertThat(harness.deliveredOffsets(RETAINED))
							.describedAs("offsets delivered from the retained partition %s%n%s",
									RETAINED, harness.diagnostics())
							.containsExactlyElementsOf(offsets(2 * BATCH)));
		}
	}

	@Test
	@DisplayName("control: the same revoke is harmless for a listener that is not async")
	void plainListenerSurvivesTheSameRevoke() {
		try (Harness harness = Harness.started(Harness.PLAIN_LISTENER)) {
			// Same records, same revoke, same partitions. Without asyncReplies there is no
			// offsetsInThisBatch, so the container never pauses itself and nothing leaks.
			harness.assignBothAndDeliver(MOVED);
			harness.awaitDeliveries(BATCH);

			harness.rebalanceTo(RETAINED);
			harness.produce(RETAINED, BATCH);

			await().atMost(SETTLE).untilAsserted(() ->
					assertThat(harness.deliveredOffsets(RETAINED))
							.describedAs("offsets delivered from the retained partition %s%n%s",
									RETAINED, harness.diagnostics())
							.containsExactlyElementsOf(offsets(BATCH)));
		}
	}

	/** The offsets a partition that received {@code count} records should have delivered. */
	private static List<Long> offsets(int count) {
		return java.util.stream.LongStream.range(0, count).boxed().toList();
	}

	/**
	 * A running {@link KafkaMessageListenerContainer} wired to a {@link MockConsumer}, plus the
	 * handful of steps the tests drive. Every step that changes the consumer's state is submitted
	 * as a poll task, so it runs on the consumer thread between two container iterations rather
	 * than racing them.
	 */
	private static final class Harness implements AutoCloseable {

		static final boolean ASYNC_LISTENER = true;

		static final boolean PLAIN_LISTENER = false;

		private final AssignmentScopedMockConsumer consumer = new AssignmentScopedMockConsumer();

		private final List<Delivery> deliveries = new CopyOnWriteArrayList<>();

		private final Map<TopicPartition, Long> nextOffset = new ConcurrentHashMap<>();

		private final KafkaMessageListenerContainer<String, String> container;

		private Harness(boolean asyncListener) {
			this.consumer.updateBeginningOffsets(Map.of(MOVED, 0L, RETAINED, 0L));
			this.consumer.setMaxPollRecords(MAX_POLL_RECORDS);
			Object listener = asyncListener
					? new AsyncReplyListener(this.deliveries)
					: new PlainListener(this.deliveries);
			this.container = buildContainer(this.consumer, listener);
		}

		static Harness started(boolean asyncListener) {
			return started(asyncListener, Long.MAX_VALUE);
		}

		/** @param maxPollRecords what {@code max.poll.records} would be on a real consumer. */
		static Harness started(boolean asyncListener, long maxPollRecords) {
			Harness harness = new Harness(asyncListener);
			harness.consumer.setMaxPollRecords(maxPollRecords);
			harness.container.start();
			return harness;
		}

		/**
		 * Assigns both partitions to this member, then puts {@link #BATCH} records on each of
		 * {@code partitionsWithRecords} for the next poll to return.
		 */
		void assignBothAndDeliver(TopicPartition... partitionsWithRecords) {
			this.consumer.schedulePollTask(() -> {
				this.consumer.rebalance(List.of(MOVED, RETAINED));
				for (TopicPartition partition : partitionsWithRecords) {
					addRecords(partition, BATCH);
				}
			});
		}

		/**
		 * The cooperative partial revoke. {@code MockConsumer.rebalance} invokes
		 * {@code onPartitionsRevoked(owned - assigned)}, then {@code assignFromSubscribed}, then
		 * {@code onPartitionsAssigned(assigned - owned)} - which for a member that only loses
		 * partitions is the empty set, so {@code repauseIfNeeded} is a no-op.
		 */
		void rebalanceTo(TopicPartition... newAssignment) {
			Set<TopicPartition> assignment = Set.of(newAssignment);
			this.consumer.schedulePollTask(() -> this.consumer.rebalance(assignment));
			await().atMost(SETTLE).until(() -> this.consumer.assignment().equals(assignment));
			// Let the iteration that performed the revoke run to completion, so
			// doResumeConsumerIfNecessary() has had its chance to resume the retained partition.
			awaitFurtherPolls(3);
		}

		/**
		 * Completes the outstanding acknowledgements and performs the revoke inside the same poll,
		 * so the container reaches no resume check in between. In production that interleaving is a
		 * race between the thread completing the acks and the poll that processes the rebalance;
		 * running both as one poll task pins it.
		 */
		void acknowledgeThenRebalanceTo(TopicPartition... newAssignment) {
			Set<TopicPartition> assignment = Set.of(newAssignment);
			this.consumer.schedulePollTask(() -> {
				List.copyOf(this.deliveries).forEach(Delivery::acknowledge);
				this.consumer.rebalance(assignment);
			});
			await().atMost(SETTLE).until(() -> this.consumer.assignment().equals(assignment));
			awaitFurtherPolls(3);
		}

		/** Puts {@code count} more records on {@code partition}, continuing from the last offset. */
		void produce(TopicPartition partition, int count) {
			this.consumer.schedulePollTask(() -> addRecords(partition, count));
		}

		/**
		 * Completes all outstanding work. An acknowledgement whose partition was revoked is either
		 * ignored or throws {@code IllegalStateException}, depending on the spring-kafka version;
		 * in production it lands in the {@code whenComplete} callback of
		 * {@code MessagingMessageListenerAdapter.handleResult}, where nothing consumes the
		 * resulting future and it is swallowed. This does the same.
		 */
		void acknowledgeEverythingInFlight() {
			List.copyOf(this.deliveries).forEach(Delivery::acknowledge);
		}

		void awaitDeliveries(int expected) {
			await().atMost(SETTLE).until(() -> this.deliveries.size() >= expected);
		}

		/**
		 * Waits until the container has paused exactly these partitions. Observable through the
		 * consumer alone: {@code doPauseConsumerIfNecessary()} sets {@code consumerPaused = true}
		 * on the statement after {@code consumer.pause(assigned)}, on the same thread, so once this
		 * returns the next poll task is guaranteed to run against a paused container.
		 */
		void awaitPaused(TopicPartition... partitions) {
			Set<TopicPartition> expected = Set.of(partitions);
			await().atMost(SETTLE).until(() -> this.consumer.paused().equals(expected));
		}

		List<Long> deliveredOffsets(TopicPartition partition) {
			return this.deliveries.stream()
					.filter(delivery -> delivery.partition().equals(partition))
					.map(Delivery::offset)
					.toList();
		}

		/** Everything a failure should print, including the container's private state. */
		String diagnostics() {
			return "  consumer.assignment() = " + this.consumer.assignment() + System.lineSeparator()
					+ "  consumer.paused()     = " + this.consumer.paused() + System.lineSeparator()
					+ "  delivered             = " + this.deliveries + System.lineSeparator()
					+ "  ListenerConsumer      = " + listenerConsumerState();
		}

		private void addRecords(TopicPartition partition, int count) {
			long from = this.nextOffset.getOrDefault(partition, 0L);
			for (long offset = from; offset < from + count; offset++) {
				this.consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),
						offset, "k" + offset, "v" + offset));
			}
			this.nextOffset.put(partition, from + count);
		}

		private void awaitFurtherPolls(int count) {
			int target = this.consumer.polls.get() + count;
			await().atMost(SETTLE).until(() -> this.consumer.polls.get() >= target);
		}

		private String listenerConsumerState() {
			try {
				Object listenerConsumer =
						read(this.container, KafkaMessageListenerContainer.class, "listenerConsumer");
				if (listenerConsumer == null) {
					return "not running";
				}
				Class<?> type = listenerConsumer.getClass();
				// The monitor ackInOrder(), captureOffsets() and onPartitionsRevoked() all hold.
				synchronized (listenerConsumer) {
					return "offsetsInThisBatch=" + read(listenerConsumer, type, "offsetsInThisBatch")
							+ " pausedForAsyncAcks=" + read(listenerConsumer, type, "pausedForAsyncAcks")
							+ " consumerPaused=" + read(listenerConsumer, type, "consumerPaused");
				}
			}
			catch (ReflectiveOperationException | RuntimeException ex) {
				return "unavailable (" + ex + ")";
			}
		}

		private static Object read(Object target, Class<?> declaringType, String name)
				throws ReflectiveOperationException {

			Field field = declaringType.getDeclaredField(name);
			field.setAccessible(true);
			return field.get(target);
		}

		@Override
		public void close() {
			this.container.stop();
		}

	}

	private static KafkaMessageListenerContainer<String, String> buildContainer(
			Consumer<String, String> consumer, Object listener) {

		Map<String, Object> consumerConfig = Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:9092",
				ConsumerConfig.GROUP_ID_CONFIG, GROUP,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);

		DefaultKafkaConsumerFactory<String, String> consumerFactory =
				new DefaultKafkaConsumerFactory<>(consumerConfig) {

					@Override
					protected Consumer<String, String> createRawConsumer(Map<String, Object> configProps) {
						return consumer;
					}

				};

		ContainerProperties containerProperties = new ContainerProperties(TOPIC);
		containerProperties.setGroupId(GROUP);
		containerProperties.setPollTimeout(20);
		// Deliberately nothing else. No ack mode, no asyncAcks, no rebalance listener, no
		// concurrency setting: the ack mode is whatever determineAckMode() derives from the
		// listener, and enable.auto.commit is the false the container forces when it is unset.
		containerProperties.setMessageListener(listener);
		return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
	}

	/**
	 * Stands in for a {@code @KafkaListener} that returns {@code CompletableFuture} / {@code Mono}
	 * / a Kotlin {@code suspend fun}: it reports {@code asyncReplies}, exactly as
	 * {@code HandlerAdapter} does for those return types, and it does not acknowledge inline
	 * because the work is still running.
	 */
	private static final class AsyncReplyListener
			implements AcknowledgingMessageListener<String, String>, AsyncRepliesAware {

		private final List<Delivery> deliveries;

		AsyncReplyListener(List<Delivery> deliveries) {
			this.deliveries = deliveries;
		}

		@Override
		public boolean isAsyncReplies() {
			return true;
		}

		@Override
		public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
			this.deliveries.add(new Delivery(data, acknowledgment));
		}

	}

	/** An ordinary listener: no async reply, so no out-of-order ack bookkeeping. */
	private static final class PlainListener implements MessageListener<String, String> {

		private final List<Delivery> deliveries;

		PlainListener(List<Delivery> deliveries) {
			this.deliveries = deliveries;
		}

		@Override
		public void onMessage(ConsumerRecord<String, String> data) {
			this.deliveries.add(new Delivery(data, null));
		}

	}

	/** One listener invocation: the record delivered and the acknowledgement handed with it. */
	private static final class Delivery {

		private final ConsumerRecord<String, String> record;

		private final Acknowledgment acknowledgment;

		Delivery(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
			this.record = record;
			this.acknowledgment = acknowledgment;
		}

		TopicPartition partition() {
			return new TopicPartition(this.record.topic(), this.record.partition());
		}

		long offset() {
			return this.record.offset();
		}

		void acknowledge() {
			if (this.acknowledgment == null) {
				return;
			}
			try {
				this.acknowledgment.acknowledge();
			}
			catch (RuntimeException ex) {
				// Swallowed in production; see acknowledgeEverythingInFlight().
			}
		}

		@Override
		public String toString() {
			return partition() + "@" + offset();
		}

	}

	/**
	 * A {@link MockConsumer} whose {@code paused()} is scoped to the current assignment, the way
	 * {@code KafkaConsumer.paused()} is - it returns {@code SubscriptionState.pausedPartitions()},
	 * which cannot contain a partition this member no longer holds. {@code MockConsumer} keeps its
	 * own {@code Set} that a rebalance never prunes, so without this a revoked partition would
	 * reach {@code doResumeConsumerIfNecessary()}'s {@code consumer.resume(consumer.paused())}
	 * sweep and throw "No current assignment for partition".
	 *
	 * <p>Nothing else is adjusted. In particular the pause flag surviving a partial revoke, which
	 * is the mechanism under test, is the real {@code SubscriptionState.assignFromSubscribed}
	 * reusing the existing {@code TopicPartitionState}.
	 */
	@SuppressWarnings({ "deprecation", "removal" })
	private static final class AssignmentScopedMockConsumer extends MockConsumer<String, String> {

		private final AtomicInteger polls = new AtomicInteger();

		AssignmentScopedMockConsumer() {
			// The String overload is 4.x-only; this one exists in both kafka-clients 3.9 and 4.x,
			// so one source tree runs against every affected spring-kafka line.
			super(OffsetResetStrategy.EARLIEST);
		}

		@Override
		public synchronized Set<TopicPartition> paused() {
			Set<TopicPartition> assignedAndPaused = new HashSet<>(super.paused());
			assignedAndPaused.retainAll(assignment());
			return assignedAndPaused;
		}

		@Override
		public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
			this.polls.incrementAndGet();
			return super.poll(timeout);
		}

	}

}
