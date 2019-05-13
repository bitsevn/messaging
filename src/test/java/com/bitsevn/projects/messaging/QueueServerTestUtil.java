package com.bitsevn.projects.messaging;

import org.junit.Assert;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class QueueServerTestUtil {

    public static QueueServer createQueueServer(List<String> producers, int eventsPerProducer) {
        final int totalEvents = eventsPerProducer * producers.size();

        BlockingQueue<QueueServer.Event> productionQ = new ArrayBlockingQueue<>(totalEvents);
        BlockingQueue<QueueServer.Event> dispatchQ = new ArrayBlockingQueue<>(totalEvents);
        BlockingQueue<QueueServer.Event> joinerQ = new ArrayBlockingQueue<>(totalEvents);

        QueueServer queueServer = new QueueServer();
        queueServer.setDebugEnabled(false);
        queueServer.setDispatchCallback((ev) -> {
            try {
                dispatchQ.put(ev);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        queueServer.setProduceCallback((ev) -> {
            try {
                productionQ.put(ev);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        queueServer.setJoinCallback((ev) -> {
            try {
                joinerQ.put(ev);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        queueServer.setShutdownCallback(() -> {
            Assert.assertEquals(totalEvents, productionQ.size());
            Assert.assertEquals(totalEvents, dispatchQ.size());
            Assert.assertEquals(totalEvents, joinerQ.size());

            final int halfEvents = totalEvents/2;

            QueueServer.Event[] abDispatchedEvents = new QueueServer.Event[halfEvents];
            QueueServer.Event[] abJoinedEvents = new QueueServer.Event[halfEvents];
            QueueServer.Event[] cdDispatchedEvents = new QueueServer.Event[halfEvents];
            QueueServer.Event[] cdJoinedEvents = new QueueServer.Event[halfEvents];
            QueueServer.Event event;
            int c1 = 0, c2 = 0;
            while (!dispatchQ.isEmpty()) {
                event = dispatchQ.poll();
                if(event.getValue().startsWith("A") || event.getValue().startsWith("B")) {
                    abDispatchedEvents[c1++] = event;
                } else if(event.getValue().startsWith("C") || event.getValue().startsWith("D")) {
                    cdDispatchedEvents[c2++] = event;
                }
            }
            c1 = 0;
            c2 = 0;
            while (!joinerQ.isEmpty()) {
                event = joinerQ.poll();
                if(event.getValue().startsWith("A") || event.getValue().startsWith("B")) {
                    abJoinedEvents[c1++] = event;
                } else if(event.getValue().startsWith("C") || event.getValue().startsWith("D")) {
                    cdJoinedEvents[c2++] = event;
                }
            }
            Assert.assertEquals(halfEvents, abDispatchedEvents.length);
            Assert.assertEquals(halfEvents, cdDispatchedEvents.length);
            Assert.assertEquals(halfEvents, abJoinedEvents.length);
            Assert.assertEquals(halfEvents, cdJoinedEvents.length);

            Assert.assertArrayEquals(abDispatchedEvents, abJoinedEvents);
            Assert.assertArrayEquals(cdDispatchedEvents, cdJoinedEvents);

            /*System.out.println("################################");
            System.out.println("##### Assertion successful #####");
            System.out.println("##### All tests passed     #####");
            System.out.println("################################");*/
        });
        return queueServer;
    }

}
