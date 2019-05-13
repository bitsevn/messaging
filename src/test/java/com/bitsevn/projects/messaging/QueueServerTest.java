package com.bitsevn.projects.messaging;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class QueueServerTest {

    @Test
    public void test_ring_size_64_events_per_producer_10_workers_no_of_cores() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 64;
        final int eventsPerProducer = 10;
        final int workers = Runtime.getRuntime().availableProcessors();
        QueueServer queueServer = QueueServerTestUtil.createQueueServer(producers, eventsPerProducer);
        queueServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    @Test
    public void test_ring_size_128_events_per_producer_100_workers_no_of_cores() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 128;
        final int eventsPerProducer = 100;
        final int workers = Runtime.getRuntime().availableProcessors();
        QueueServer queueServer = QueueServerTestUtil.createQueueServer(producers, eventsPerProducer);
        queueServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    @Test
    public void test_ring_size_1024_events_per_producer_1000_workers_8() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 1024;
        final int eventsPerProducer = 1000;
        final int workers = 8;
        QueueServer queueServer = QueueServerTestUtil.createQueueServer(producers, eventsPerProducer);
        queueServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    @Test
    public void test_ring_size_4096_events_per_producer_5000_workers_8() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 4096;
        final int eventsPerProducer = 5000;
        final int workers = 8;
        QueueServer queueServer = QueueServerTestUtil.createQueueServer(producers, eventsPerProducer);
        queueServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    @Test
    public void test_ring_size_32768_events_per_producer_10000_workers_16() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 32768;
        final int eventsPerProducer = 10000;
        final int workers = 16;
        QueueServer queueServer = QueueServerTestUtil.createQueueServer(producers, eventsPerProducer);
        queueServer.start(ringSize, eventsPerProducer, workers, producers);
    }

}
