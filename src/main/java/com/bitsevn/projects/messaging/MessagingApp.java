package com.bitsevn.projects.messaging;

import com.bitsevn.projects.messaging.callback.Acknowledgement;
import com.bitsevn.projects.messaging.callback.ExitCallback;
import com.bitsevn.projects.messaging.consumer.DataStreamConsumer;
import com.bitsevn.projects.messaging.dispatcher.DataStreamDispatcher;
import com.bitsevn.projects.messaging.extractor.DataStreamResultExtractor;
import com.bitsevn.projects.messaging.producer.DataStreamProducer;
import com.google.common.base.Stopwatch;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class MessagingApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingApp.class);

    public static void main(String[] args) {

        final int FIRST_QUEUE_CAPACITY = 1000;
        final int AB_QUEUE_CAPACITY = 300;
        final int CD_QUEUE_CAPACITY = 300;
        final int RESULT_QUEUE_CAPACITY = 1000;
        final int STREAMS_PER_STREAM_GROUP = 1000;
        final int MAX_THREADS_PER_CONSUMER = 10;

        List<String> STREAM_GROUPS = Arrays.asList("A", "B", "C", "D");
        // shuffle the list to produce random order or stream generation
        // Collections.shuffle(streamGroups);

        new MessagingApp().run(
                FIRST_QUEUE_CAPACITY,
                AB_QUEUE_CAPACITY,
                CD_QUEUE_CAPACITY,
                RESULT_QUEUE_CAPACITY,
                STREAMS_PER_STREAM_GROUP,
                MAX_THREADS_PER_CONSUMER,
                STREAM_GROUPS);


    }

    public void run(
            final int first_queue_capacity,
            final int ab_queue_capacity,
            final int cd_queue_capacity,
            final int result_queue_capacity,
            final int streams_per_stream_group,
            int max_threads_per_consumer,
            final List<String> stream_groups) {
        runAndAssert(
                first_queue_capacity,
                ab_queue_capacity,
                cd_queue_capacity,
                result_queue_capacity,
                streams_per_stream_group,
                max_threads_per_consumer,
                stream_groups,
                false);
    }

    /**
     * Using ArrayBlockingQueue so that we can specify data structure capacity. This uses array implementation
     * under the hood, that makes it memory efficient and cache-friendly for modern processors
     *
     * @param first_queue_capacity - max capacity of the first level queue where stream producers will send messages
     * @param ab_queue_capacity - max capacity of read queue for A/B streams
     * @param cd_queue_capacity - max capacity of read queue for C/D streams
     * @param result_queue_capacity - max capacity of result queue
     * @param streams_per_stream_group - no. of streams to be produced per stream group
     * @param max_threads_per_consumer - no. of concurrent threads to be used in executor service per consumer
     * @param stream_groups - list of stream groups
     */
    public void runAndAssert(
            final int first_queue_capacity,
            final int ab_queue_capacity,
            final int cd_queue_capacity,
            final int result_queue_capacity,
            final int streams_per_stream_group,
            int max_threads_per_consumer,
            final List<String> stream_groups,
            final boolean assertions) {

        // stream producers will write to this queue
        BlockingQueue<String> readQueueAll = new ArrayBlockingQueue<>(first_queue_capacity);

        // consumer of A/B streams will read this queue and this queue will be written by {@link DataStreamDispatcher}
        BlockingQueue<String> readQueueAB = new ArrayBlockingQueue<>(ab_queue_capacity);

        // consumer of C/D streams will read this queue and this queue will be written by {@link DataStreamDispatcher}
        BlockingQueue<String> readQueueCD = new ArrayBlockingQueue<>(cd_queue_capacity);

        /**
         * results of stream consumers will be written to this queue.
         * Note that we are storing Future<T> object in the blocking queue.
         * This will help us in retrieving the stream results in the order in which they were produced/consumed.
         * Also, if one stream gets processed before other stream even when it arrived later, will also maintain the order
         * as Future will wait on unfinished streams!
         */
        BlockingQueue<Future<String>> resultQueue = new ArrayBlockingQueue<>(result_queue_capacity);

        /**
         * START - ASSERTION
         * Below lines for idea verification and assertion of the expected system behaviour
         */
        ExitCallback exitCallback = null;
        Acknowledgement dispatchAck = null;
        Acknowledgement processedAck = null;
        if(assertions) {
            BlockingQueue<String> dispatchedStreams = new LinkedBlockingQueue<>(stream_groups.size() * streams_per_stream_group);
            BlockingQueue<String> processedStreams = new LinkedBlockingQueue<>(stream_groups.size() * streams_per_stream_group);
            dispatchAck = (stream) -> {
                try {
                    dispatchedStreams.put(stream);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
            processedAck = (stream) -> {
                try {
                    processedStreams.put(stream);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
            exitCallback = new ExitCallbackHandler(dispatchedStreams, processedStreams);
        }
        /**
         * END - ASSERTION
         */

        /**
         * Create one publisher per stream and run it in a separate thread,
         * imitating the behaviour that stream production of a stream group is independent of each other.
         */
        for(String streamGroup: stream_groups) {
            String threadGroup = "A".equals(streamGroup) || "B".equals(streamGroup) ? "AB" : "CD";
            new Thread(new DataStreamProducer(readQueueAll, streamGroup, streams_per_stream_group), "thread-publisher-" + threadGroup).start();
        }

        /**
         * Create just one dispatcher as it will be responsible for orchestrating/splitting/routing the streams
         * from group A/B for one consumer and C/D to the other.
         */
        new Thread(new DataStreamDispatcher(readQueueAll, readQueueAB, readQueueCD, dispatchAck, stream_groups.size()), "thread-dispatcher-all").start();


        /**
         * Create two consumers - one for A/B streams and other for C/D streams.
         * They both run in their own threads.
         * Each data stream consumer has a executor service that responsible for submitting stream processing in a separate thread.
         */
        new Thread(new DataStreamConsumer(readQueueAB, resultQueue, "consumer-AB", max_threads_per_consumer, 2), "thread-consumer-AB").start();
        new Thread(new DataStreamConsumer(readQueueCD, resultQueue, "consumer-CD", max_threads_per_consumer, 2), "thread-consumer-CD").start();


        /**
         * Finally, create result extractor, that extracts results from {@link resultQueue}
         */
        new Thread(new DataStreamResultExtractor(resultQueue, stream_groups.size(), processedAck, exitCallback), "thread-result").start();
    }

    /**
     * Once all messages are produced, consumed and result is extracted, then
     * checks assertions to ensure that the order of dispatched streams is same a resultant streams
     */
    static class ExitCallbackHandler implements ExitCallback {

        private BlockingQueue<String> dispatchedStreams;
        private BlockingQueue<String> processedStreams;
        private List<String> dispatchedAB;
        private List<String> processedAB;
        private List<String> dispatchedCD;
        private List<String> processedCD;
        private Stopwatch stopwatch;
        public ExitCallbackHandler(
                BlockingQueue<String> dispatchedStreams,
                BlockingQueue<String> processedStreams) {
            this.dispatchedStreams = dispatchedStreams;
            this.processedStreams = processedStreams;
            this.dispatchedAB = new ArrayList<>();
            this.processedAB = new ArrayList<>();
            this.dispatchedCD = new ArrayList<>();
            this.processedCD = new ArrayList<>();
            this.stopwatch = Stopwatch.createStarted();
        }

        @Override
        public void onExit() {
            this.stopwatch.stop();
            while (!dispatchedStreams.isEmpty()) {
                try {
                    String stream = dispatchedStreams.take();
                    if(stream.startsWith("A") || stream.startsWith("B")) {
                        dispatchedAB.add(stream);
                    } else {
                        dispatchedCD.add(stream);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            while (!processedStreams.isEmpty()) {
                try {
                    String stream = processedStreams.take();
                    if(stream.startsWith("A") || stream.startsWith("B")) {
                        processedAB.add(stream);
                    } else {
                        processedCD.add(stream);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            LOGGER.debug("--------------------------------------------------------------------");
            LOGGER.debug("Produced  AB: {}", dispatchedAB);
            LOGGER.debug("Processed AB: {}", processedAB);
            LOGGER.debug("Produced  CD: {}", dispatchedCD);
            LOGGER.debug("Processed CD: {}", processedCD);
            LOGGER.debug("--------------------------------------------------------------------");

            Assert.assertEquals(dispatchedAB, processedAB);
            Assert.assertEquals(dispatchedCD, processedCD);

            LOGGER.info("--------------------------------------------------------------------");
            LOGGER.info("AB stream size: [produced = {} | processed = {}]", dispatchedAB.size(), processedAB.size());
            LOGGER.info("CD stream size: [produced = {} | processed = {}]", dispatchedCD.size(), processedCD.size());
            LOGGER.info("--------------------------------------------------------------------");
            LOGGER.info("Completed. Time taken: {}", stopwatch);
            LOGGER.info("--------------------------------------------------------------------");
        }
    }

}
