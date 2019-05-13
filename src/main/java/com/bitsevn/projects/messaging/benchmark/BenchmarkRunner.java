package com.bitsevn.projects.messaging.benchmark;

import com.bitsevn.projects.messaging.MessagingApp;
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
        public String BUFFER_SIZE;

        @Param({ "100", "200", "300", "500", "1000" })
        public String STREAMS;

        @Param({ "4", "8", "16", "32" })
        public int WORKER_THREADS;

        public List<String> STREAM_GROUPS = Arrays.asList("A", "B", "C", "D");

        public MessagingApp messagingApp;

        @Setup(Level.Invocation)
        public void setup() {
            messagingApp = new MessagingApp();
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1, jvmArgsAppend = { "-Xms128M", "-Xmx2G" })
    @Measurement(iterations = 2)
    @Warmup(iterations = 3)
    @BenchmarkMode(Mode.Throughput)
    public void benchmarkThroughput(ExecutionPlan plan) {
        plan.messagingApp.run(
                Integer.valueOf(plan.BUFFER_SIZE),
                Integer.valueOf(plan.STREAMS),
                Integer.valueOf(plan.WORKER_THREADS),
                plan.STREAM_GROUPS);
    }
}
