package com.bitsevn.projects.messaging.handler;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class imitates the behaviour of some business logic processing.
 * It uses random delays in range of 10 microseconds to imitate time taking tasks.
 */
public class StreamHandler implements Callable<String> {

    private String stream;

    public StreamHandler(String stream) {
        this.stream = stream;
    }

    @Override
    public String call() {
        int delay = ThreadLocalRandom.current().nextInt(50);
        while (delay > 0) {
            delay--; // busy spin
        }
        return String.format("%s:processed", stream);
    }
}
