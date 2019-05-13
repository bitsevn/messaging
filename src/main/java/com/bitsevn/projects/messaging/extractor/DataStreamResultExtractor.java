package com.bitsevn.projects.messaging.extractor;

import com.bitsevn.projects.messaging.callback.Acknowledgement;
import com.bitsevn.projects.messaging.callback.ExitCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is responsible for extracting results from result queue {@link ArrayBlockingQueue<Future<String>>}.
 * Currently it just invokes an {@link Acknowledgement} callback - but, this could be anything like I/O write, file write, etc.
 */
public class DataStreamResultExtractor implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(DataStreamResultExtractor.class);
    private AtomicInteger totalStreamGroups;
    private String extractorId;
    private BlockingQueue<Future<String>> readQueue;
    private Acknowledgement acknowledgement;
    private ExitCallback exitCallback;

    public DataStreamResultExtractor(
            BlockingQueue<Future<String>> readQueue,
            int totalStreamGroups) {
        this(readQueue, totalStreamGroups, null, null);
    }

    public DataStreamResultExtractor(
            BlockingQueue<Future<String>> readQueue,
            int totalStreamGroups,
            Acknowledgement acknowledgement,
            ExitCallback exitCallback) {
        this.totalStreamGroups = new AtomicInteger(totalStreamGroups);
        this.extractorId = "result-extractor";
        this.readQueue = readQueue;
        this.acknowledgement = acknowledgement;
        this.exitCallback = exitCallback;
    }

    @Override
    public void run() {
        try {
            while (true) {
                String result = this.readQueue.take().get();
                if(!result.contains("!") && this.acknowledgement != null) {
                    this.acknowledgement.acknowledge(result.split(":")[0]);
                }
                LOGGER.debug("[{}] processed stream {}", extractorId, result);

                // termination condition for result extractor
                if(result.contains("!") && this.totalStreamGroups.decrementAndGet() <= 0) {
                    LOGGER.debug("[{}] terminating now", extractorId);
                    if(this.exitCallback != null) {
                        this.exitCallback.onExit();
                    }
                    return;
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            if(e instanceof ExecutionException) {
                LOGGER.warn("[{}] error while getting result from stream", extractorId);
            } else {
                LOGGER.warn("[{}] thread interrupted while getting result from stream", extractorId);
                Thread.currentThread().interrupt();
            }
        }
    }
}
