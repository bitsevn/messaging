package com.bitsevn.projects.messaging.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public class DataStreamProducer implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(DataStreamProducer.class);
    private final static String END_OF_STREAM = "!";

    private String producerId;
    private String streamGroup;
    private long totalStreamsToProduce;
    private BlockingQueue<String> queue;

    public DataStreamProducer(
            BlockingQueue<String> queue,
            String streamGroup,
            long totalStreamsToProduce) {
        this.producerId = String.format("publisher-%s", streamGroup);
        this.queue = queue;
        this.streamGroup = streamGroup;
        this.totalStreamsToProduce = totalStreamsToProduce;
    }

    @Override
    public void run() {
        String stream;
        for(int i=1; i<=totalStreamsToProduce; i++) {
            try {
                stream = streamGroup + i;
                queue.put(stream);
                LOGGER.debug("[{}] produced stream {}", producerId, stream);
                int delay = ThreadLocalRandom.current().nextInt(50);
                while (delay > 0) {
                    delay--; // busy spin
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            stream = streamGroup + END_OF_STREAM;
            queue.put(stream);
            LOGGER.debug("[{}] produced stream %s as end of stream group %s", producerId, stream, streamGroup);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
