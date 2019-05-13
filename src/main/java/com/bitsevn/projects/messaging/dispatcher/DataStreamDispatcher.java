package com.bitsevn.projects.messaging.dispatcher;

import com.bitsevn.projects.messaging.callback.Acknowledgement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is responsible for orchestrating/splitting/routing the streams
 * from group A/B for one consumer and C/D to the other.
 * It's a single threaded process
 */
public class DataStreamDispatcher implements Runnable {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataStreamDispatcher.class);

    /**
     * Atomic variable is used to keep track of end of stream groups indications.
     * As this variable becomes zero, consumer thread finishes.
     */
    private AtomicInteger totalStreamGroups;

    private String dispatcherId;
    private BlockingQueue<String> readQueue;
    private BlockingQueue<String> writeQueueAB;
    private BlockingQueue<String> writeQueueCD;
    private Acknowledgement acknowledgement;

    public DataStreamDispatcher(
            BlockingQueue<String> readQueue,
            BlockingQueue<String> writeQueueAB,
            BlockingQueue<String> writeQueueCD,
            Acknowledgement acknowledgement,
            int totalStreamGroups) {
        this.totalStreamGroups = new AtomicInteger(totalStreamGroups);
        this.dispatcherId = "dispatcher";
        this.readQueue = readQueue;
        this.writeQueueAB = writeQueueAB;
        this.writeQueueCD = writeQueueCD;
        this.acknowledgement = acknowledgement;
    }

    @Override
    public void run() {
        String stream = null;
        try {
            while (true) {
                stream = readQueue.take(); // blocking
                if(!stream.contains("!") && acknowledgement != null) {
                    acknowledgement.acknowledge(stream);
                }
                dispatch(stream);
                LOGGER.debug("[{}] dispatched stream {}", dispatcherId, stream);
                // termination condition for stream dispatcher
                if(stream.contains("!") && totalStreamGroups.decrementAndGet() <= 0) {
                    LOGGER.debug("[{}] terminating now", dispatcherId);
                    return;
                }
            }
        } catch (InterruptedException | IllegalArgumentException e) {
            if(e instanceof IllegalArgumentException) {
                LOGGER.warn("[{}] error while dispatching stream {}", dispatcherId, stream);
            } else {
                LOGGER.warn("[{}] thread interrupted while dispatching stream {}", dispatcherId, stream);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void dispatch(String stream) throws InterruptedException {
        if(stream.startsWith("A") || stream.startsWith("B")) {
            writeQueueAB.put(stream);
        } else if(stream.startsWith("C") || stream.startsWith("D")) {
            writeQueueCD.put(stream);
        } else {
            throw new IllegalArgumentException(String.format("Invalid stream found: %s", stream));
        }
    }
}
