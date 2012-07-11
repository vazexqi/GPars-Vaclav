// GPars - Groovy Parallel Systems
//
// Copyright © 2008-2012  The original author or authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package groovyx.gpars.benchmark.akka;

import com.google.caliper.Param;
import com.google.caliper.api.VmParam;
import com.google.caliper.runner.CaliperMain;
import groovyx.gpars.actor.DynamicDispatchActor;
import groovyx.gpars.group.DefaultPGroup;

import java.util.concurrent.CountDownLatch;

public class BenchmarkThroughputComputationDynamicActorCaliper extends BenchmarkCaliper {
    @Param({"1", "2", "4", "6", "8",
            "10", "12", "14", "16", "18",
            "20", "22", "24", "26", "28",
            "30", "32", "34", "36", "38",
            "40", "42", "44", "46", "48"}
    )
    int numberOfClients;
    @VmParam String server;
    @VmParam String xms;
    @VmParam String xmx;
    @VmParam String gc;


    BenchmarkThroughputComputationDynamicActorCaliper(){
        super(500, DYNAMIC_RUN, ComputationDynamicClient.class, ComputationDynamicDestination.class);
    }

    public long timeDynamicDispatchActorThroughput(int reps) {
        long time=0;
        try {
            time = super.timeThroughput(reps, numberOfClients);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return time;
    }

    public static void main(String[] args) {
        CaliperMain.main(BenchmarkThroughputComputationDynamicActorCaliper.class, args);
    }


}


class ComputationDynamicClient extends DynamicDispatchActor {
    private double _pi = 0.0;
    private long currentPosition = 0L;
    int nrOfElements = 1000;

    long sent = 0L;
    long received = 0L;
    ComputationDynamicDestination actor;
    long repeatsPerClient;
    CountDownLatch latch;

    public ComputationDynamicClient(ComputationDynamicDestination actor, CountDownLatch latch, long repeatsPerClient, DefaultPGroup group) {
        this.parallelGroup = group;
        this.actor = actor;
        this.repeatsPerClient = repeatsPerClient;
        this.latch = latch;
    }

    void onMessage(final DynamicMessage msg) {
        received += 1;
        calculatePi();
        if (sent < repeatsPerClient) {
            actor.send(msg);
            sent += 1;
        } else if (received >= repeatsPerClient) {
            latch.countDown();
        }
    }

    void onMessage(final DynamicRun msg) {
        for (int i = 0; i < Math.min(repeatsPerClient, 1000L); i++) {
            actor.send(BenchmarkCaliper.DYNAMIC_MESSAGE);
            sent += 1;
        }
    }

    void calculatePi() {
        _pi += calculateDecimals(currentPosition);
        currentPosition += nrOfElements;
    }

    private double calculateDecimals(long start) {
        double acc = 0.0;
        for (long i = start; i < start + nrOfElements; i++)
            acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
        return acc;
    }
}

class ComputationDynamicDestination extends DynamicDispatchActor {

    private double _pi = 0.0;
    private long currentPosition = 0L;
    int nrOfElements = 1000;

    public ComputationDynamicDestination(DefaultPGroup group) {
        this.parallelGroup = group;
    }

    void onMessage(final DynamicMessage msg) {
        calculatePi();
        getSender().send(msg);
    }

    void calculatePi() {
        _pi += calculateDecimals(currentPosition);
        currentPosition += nrOfElements;
    }

    private double calculateDecimals(long start) {
        double acc = 0.0;
        for (long i = start; i < start + nrOfElements; i++)
            acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
        return acc;
    }

}
