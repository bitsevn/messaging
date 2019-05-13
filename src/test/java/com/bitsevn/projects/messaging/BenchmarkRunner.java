package com.bitsevn.projects.messaging;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BenchmarkRunner {

    public static void main(String[] args) throws IOException, RunnerException {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        @Param({ "1024", "4096", "32768" })
        public String RING_SIZE;

        @Param({ "100", "1000", "10000", "100000" })
        public String EVENTS_PER_PRODUCER;

        @Param({ "8", "16", "32" })
        public int WORKERS;

        public List<String> PRODUCERS = Arrays.asList("A:AB", "B:AB", "C:CD", "D:CD");

        public QueueServer queueServer;

        @Setup(Level.Invocation)
        public void setUp() {
            queueServer = new QueueServer();
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1, jvmArgsAppend = { "-Xms128M", "-Xmx2G" })
    @Measurement(iterations = 2)
    @Warmup(iterations = 3)
    @BenchmarkMode(Mode.Throughput)
    public void benchmarkThroughput(ExecutionPlan plan) {
        plan.queueServer.start(
                Integer.valueOf(plan.RING_SIZE),
                Integer.valueOf(plan.EVENTS_PER_PRODUCER),
                Integer.valueOf(plan.WORKERS),
                plan.PRODUCERS);
    }
}
