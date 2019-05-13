package com.bitsevn.projects.messaging.consumer;

import com.bitsevn.projects.messaging.handler.StreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is responsible for handling a data stream.
 * It submits the incoming data stream processing into a separate thread using executor service.
 * Executor service accepts {@link Callable<String>} objects and adds it's result {@link Future<String>} into
 * a result queue.
 */
public class DataStreamConsumer implements Runnable {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataStreamConsumer.class);

    /**
     * Atomic variable is used to keep track of end of stream indications.
     * As this variable becomes zero, consumer thread finishes.
     */
    private AtomicInteger totalStreamGroups;

    private String consumerId;
    private ExecutorService executorService;
    private BlockingQueue<String> readQueue;
    private BlockingQueue<Future<String>> writeQueue;

    public DataStreamConsumer(
            BlockingQueue<String> readQueue,
            BlockingQueue<Future<String>> writeQueue,
            String consumerId,
            int maxThreads,
            int totalStreamGroups) {
        this.totalStreamGroups = new AtomicInteger(totalStreamGroups);
        this.readQueue = readQueue;
        this.writeQueue = writeQueue;
        this.consumerId = consumerId;
        this.executorService = Executors.newFixedThreadPool(maxThreads);
    }

    @Override
    public void run() {
        try {
            while (true) {
                // this is a thread-safe blocking call
                String stream = readQueue.take();
                Future<String> future = executorService.submit(new StreamHandler(stream));
                writeQueue.put(future);
                // termination condition for stream consumer
                LOGGER.debug("[{}] submitted stream {} for processing", consumerId, stream);
                if(stream.contains("!") && totalStreamGroups.decrementAndGet() <= 0) {
                    LOGGER.debug("[{}] terminating now", consumerId);
                    executorService.awaitTermination(2, TimeUnit.SECONDS);
                    executorService.shutdown();
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
