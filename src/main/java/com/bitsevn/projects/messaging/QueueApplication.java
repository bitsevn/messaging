package com.bitsevn.projects.messaging;

import java.util.Arrays;

public class QueueApplication {

    public static void main(String[] args) {
        QueueServer queueServer = new QueueServer();
        queueServer.setDebugEnabled(true);
        queueServer.start(100, 2, 4, Arrays.asList("A", "B", "C", "D"));
    }
}
