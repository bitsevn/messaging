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

        // @Param({ "100", "1000", "10000", "100000", "1000000", "10000000", "50000000" })
        @Param({"1000", "10000", "100000", "1000000", "10000000"})
        public String streamsPerStreamGroup;

        // @Param({ "10", "100", "1000", "10000", "100000", "1000000", "50000000" })
        @Param({"1000", "10000", "100000", "1000000"})
        public String queueCapacity;

        @Param({"8", "16", "32"})
        public String maxThreads;

        public MessagingApp messagingApp;

        private static final List<String> STREAM_GROUPS = Arrays.asList("A", "B", "C", "D");

        @Setup(Level.Invocation)
        public void setup() {
            messagingApp = new MessagingApp();
        }
    }

    @Benchmark
    @Fork(value = 5, warmups = 2, jvmArgsAppend = { "-Xms128M", "-Xmx2G" })
    @Measurement(iterations = 5)
    @BenchmarkMode(Mode.Throughput)
    public void benchmarkThroughput(ExecutionPlan plan) {
        plan.messagingApp.run(
                Integer.valueOf(plan.queueCapacity),
                Integer.valueOf(plan.queueCapacity),
                Integer.valueOf(plan.queueCapacity),
                Integer.valueOf(plan.queueCapacity),
                Integer.valueOf(plan.streamsPerStreamGroup),
                Integer.valueOf(plan.maxThreads),
                plan.STREAM_GROUPS);
    }
}
