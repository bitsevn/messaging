package com.bitsevn.projects.messaging;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueServer {

    private boolean debugEnabled = true;
    private ProduceCallback produceCallback;
    private DispatchCallback dispatchCallback;
    private JoinCallback joinCallback;
    private ShutdownCallback shutdownCallback;

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public void setDebugEnabled(boolean debugEnabled) {
        this.debugEnabled = debugEnabled;
    }

    public ProduceCallback getProduceCallback() {
        return produceCallback;
    }

    public void setProduceCallback(ProduceCallback produceCallback) {
        this.produceCallback = produceCallback;
    }

    public DispatchCallback getDispatchCallback() {
        return dispatchCallback;
    }

    public void setDispatchCallback(DispatchCallback dispatchCallback) {
        this.dispatchCallback = dispatchCallback;
    }

    public JoinCallback getJoinCallback() {
        return joinCallback;
    }

    public void setJoinCallback(JoinCallback joinCallback) {
        this.joinCallback = joinCallback;
    }

    public ShutdownCallback getShutdownCallback() {
        return shutdownCallback;
    }

    public void setShutdownCallback(ShutdownCallback shutdownCallback) {
        this.shutdownCallback = shutdownCallback;
    }

    interface ProduceCallback {
        void postProduce(Event e);
    }

    interface DispatchCallback {
        void postDispatch(Event e);
    }

    interface JoinCallback {
        void postJoin(Event e);
    }

    interface ShutdownCallback {
        void preShutdown();
    }

    static class Event {
        private String value;
        private String result;

        public Event() {}

        public Event(String value) {
            this.value = value;
        }

        public Event(String value, String result) {
            this.value = value;
            this.result = result;
        }

        @Override
        public Event clone() {
            return new Event(value, result);
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        public void clear() {
            this.value = null;
            this.result = null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;
            return Objects.equals(value, event.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public void start(int ringSize, int eventsPerProducer, int workers, List<String> producers) {
        final int totalEvents = producers.size() * eventsPerProducer;
        CountDownLatch gate = new CountDownLatch(totalEvents);

        // stream producers will write to this queue
        BlockingQueue<Event> milestone = new ArrayBlockingQueue<>(ringSize);

        // consumer of A/B streams will read this queue and this queue will be written by {@link DataStreamDispatcher}
        BlockingQueue<Event> milestoneAB = new ArrayBlockingQueue<>(ringSize);

        // consumer of C/D streams will read this queue and this queue will be written by {@link DataStreamDispatcher}
        BlockingQueue<Event> milestoneCD = new ArrayBlockingQueue<>(ringSize);

        /**
         * results of stream consumers will be written to this queue.
         * Note that we are storing Future<T> object in the blocking queue.
         * This will help us in retrieving the stream results in the order in which they were produced/consumed.
         * Also, if one stream gets processed before other stream even when it arrived later, will also maintain the order
         * as Future will wait on unfinished streams!
         */
        BlockingQueue<Future<Event>> joinerMilestone = new ArrayBlockingQueue<>(totalEvents);

        /**
         * This class is responsible for orchestrating/splitting/routing the streams
         * from group A/B for one consumer and C/D to the other.
         * It's a single threaded process
         */
        AtomicInteger totalEventsToDispatch = new AtomicInteger(totalEvents);
        Runnable dispatchHandler = () -> {
            while(totalEventsToDispatch.decrementAndGet() >= 0) {
                try {
                    Event ev = milestone.take();
                    if(ev.value.startsWith("A") || ev.value.startsWith("B")) {
                        milestoneAB.put(ev);
                    } else if(ev.value.startsWith("C") || ev.value.startsWith("D")) {
                        milestoneCD.put(ev);
                    }
                    if(debugEnabled) {
                        System.out.println(String.format("[dispatcher] dispatched event %s", ev));
                    }
                    if(dispatchCallback != null) {
                        dispatchCallback.postDispatch(ev);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(debugEnabled) {
                System.out.println("[dispatcher] terminating now");
            }
        };

        /**
         * This is responsible for handling a data stream.
         * It submits the incoming data stream processing into a separate thread using executor service.
         * Executor service accepts {@link Callable<Future<Event>>} objects and adds it's result {@link Future<String>} into
         * a result queue.
         */
        ExecutorService abWorkerService = Executors.newFixedThreadPool(workers);
        Runnable abConsumer = getConsumer("ab", milestoneAB, joinerMilestone, abWorkerService, new AtomicInteger(totalEvents/2));

        ExecutorService cdWorkerService = Executors.newFixedThreadPool(workers);
        Runnable cdConsumer = getConsumer("cd", milestoneCD, joinerMilestone, abWorkerService, new AtomicInteger(totalEvents/2));

        /**
         * Finally, create result extractor, that extracts results from {@link resultQueue}
         */
        AtomicInteger totalEventsToJoin = new AtomicInteger(totalEvents);
        Runnable joinerConsumer = () -> {
            try {
                while (totalEventsToJoin.decrementAndGet() >= 0) {
                    Event event = joinerMilestone.take().get();// blocking action
                    if(debugEnabled) {
                        System.out.println(String.format("[joiner] joined event %s", event));
                    }
                    if(joinCallback != null) {
                        joinCallback.postJoin(event);
                    }
                    gate.countDown();
                }
                if(debugEnabled) {
                    System.out.println("[joiner] terminating now");
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        };

        // start process threads and wait for events to flow in
        new Thread(joinerConsumer).start();
        new Thread(abConsumer).start();
        new Thread(cdConsumer).start();
        new Thread(dispatchHandler).start();

        // finally, set up producers
        for(int p=0; p<producers.size(); p++) {
            final String producer = producers.get(p);
            new Thread(() -> {
                for(int i=0; i<eventsPerProducer; i++) {
                    try {
                        final int eventId = i + 1;
                        Event event = new Event(String.format("%s%s", producer, eventId));
                        milestone.put(event);
                        if(debugEnabled) {
                            System.out.println(String.format("[producer-%s] produced event %s", producer, event));
                        }
                        if(produceCallback != null) produceCallback.postProduce(event.clone());
                        TimeUnit.MICROSECONDS.sleep(ThreadLocalRandom.current().nextInt(20));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, String.format("producer-%s-thread", producer)).start();
        }

        try {
            gate.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(shutdownCallback != null) shutdownCallback.preShutdown();
            abWorkerService.shutdown();
            cdWorkerService.shutdown();
        }
    }

    private Runnable getConsumer(String id, BlockingQueue<Event> milestone, BlockingQueue<Future<Event>> joinerMilestone, ExecutorService workerService, AtomicInteger totalABEventsToConsume) {
        return () -> {
            try {
                while (totalABEventsToConsume.decrementAndGet() >= 0) {
                    Event event = milestone.take();
                    joinerMilestone.put(workerService.submit(() -> {
                        event.setResult("processed");
                        int delay = ThreadLocalRandom.current().nextInt(20);
                        while (delay > 0) {
                            delay--; // busy spin
                        }
                        return event;
                    }));
                    if(debugEnabled) {
                        System.out.println(String.format("[consumer-%s] consumed event %s", id, event));
                    }
                }
                if(debugEnabled) {
                    System.out.println(String.format("[consumer-%s] terminating now", id));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }
}
