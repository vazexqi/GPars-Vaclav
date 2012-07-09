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
import groovyx.gpars.actor.StaticDispatchActor;
import groovyx.gpars.group.DefaultPGroup;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;

<<<<<<< HEAD
public class BenchmarkThroughputComputationStaticActorCaliper extends BenchmarkCaliper {

    @Param({"1", "2", "4", "6", "8",
=======
public class BenchmarkThroughputComputationStaticActorCaliper extends Benchmark {
    @Param({"1", "2", "4"/*, "6", "8",
>>>>>>> origin/Temp-ServerMachineTest
            "10", "12", "14", "16", "18",
            "20", "22", "24", "26", "28",
            "30", "32", "34", "36", "38",
            "40", "42", "44", "46", "48"*/}
    )
    int numberOfClients;
<<<<<<< HEAD
    @VmParam({"-server"})
    String server;
    @VmParam({"-Xms512M"})
    String xms;
    @VmParam({"-Xmx1024M"})
    String xmx;
    @VmParam({"-XX:+UseParallelGC"})
    String gc;

    BenchmarkThroughputComputationStaticActorCaliper(){
        super(500, BenchmarkCaliper.STATIC_RUN, ComputationStaticClient.class, ComputationStaticDestination.class);
    }
=======


    @VmParam({"-server"}) String server;
    @VmParam({"-Xms512M"}) String xms;
    @VmParam({"-Xmx1024M"}) String xmx;
    @VmParam({"-XX:+UseParallelGC"}) String gc;

    int maxClients = 4;
    public static final int RUN = 1;
    public static final int MESSAGE = 2;
    final int maxRunDurationMillis = 20000;
    DefaultPGroup group;
    long repeatsPerClient;
    int repeatFactor = 500;
    int repeat = 500 * repeatFactor; //total number of messages that needs to be sent
>>>>>>> origin/Temp-ServerMachineTest

    public int totalMessages() {
        return repeat;
    }

    public long timeComputationStaticDispatchActorThroughput(int reps) {
<<<<<<< HEAD
        long time=0;
        try {
            time = super.timeThroughput(reps, numberOfClients);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
=======
        long totalTime = 0;
        group = new DefaultPGroup(new FJPool(maxClients));
        repeatsPerClient = repeat / numberOfClients;//MESSAGE quota for each pair of actors

        for (int i = 0; i < reps; i++) {

            CountDownLatch latch = new CountDownLatch(numberOfClients);
            ArrayList<ComputationDestinationActor> destinations = new ArrayList<ComputationDestinationActor>();
            ArrayList<ComputationClientActor> clients = new ArrayList<ComputationClientActor>();

            for (int j = 0; j < numberOfClients; j++) {
                destinations.add((ComputationDestinationActor) new ComputationDestinationActor(group).start());

            }
            for (ComputationDestinationActor destination : destinations) {
                clients.add((ComputationClientActor) new ComputationClientActor(destination, latch, repeatsPerClient, group).start());
            }

            long startTime = System.nanoTime();//start timing

            for (ComputationClientActor client : clients) {
                client.send(RUN);
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            totalTime += System.nanoTime() - startTime;//stop timing

            for (ComputationClientActor client : clients) {
                client.terminate();
            }
            for (ComputationDestinationActor destination : destinations) {
                destination.terminate();
            }
>>>>>>> origin/Temp-ServerMachineTest
        }
        return time;
    }

    public static void main(String[] args) {
        CaliperMain.main(BenchmarkThroughputComputationStaticActorCaliper.class, args);
    }

}

class ComputationStaticClient extends StaticDispatchActor<Integer> {

    long sent = 0L;
    long received = 0L;
    ComputationStaticDestination actor;
    long repeatsPerClient;
    CountDownLatch latch;

    private double _pi = 0.0;
    private long currentPosition = 0L;
    int nrOfElements = 1000;


    public ComputationStaticClient(ComputationStaticDestination actor, CountDownLatch latch, long repeatsPerClient, DefaultPGroup group) {
        this.parallelGroup = group;
        this.actor = actor;
        this.repeatsPerClient = repeatsPerClient;
        this.latch = latch;
    }

    @Override
    public void onMessage(Integer msg) {
        if (msg.equals(BenchmarkCaliper.STATIC_MESSAGE)) {
            received += 1;
            calculatePi();
            if (sent < repeatsPerClient) {
                actor.send(msg);
                sent += 1;
            } else if (received >= repeatsPerClient) {
                latch.countDown();
                sent = 0;
                received = 0;
            }
        }

        if (msg.equals(BenchmarkCaliper.STATIC_RUN)) {
            for (int i = 0; i < (Math.min(repeatsPerClient, 1000L)); i++) {
                actor.send(BenchmarkCaliper.STATIC_MESSAGE);
                sent += 1;
            }
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

class ComputationStaticDestination extends StaticDispatchActor<Integer> {
    private double _pi = 0.0;
    private long currentPosition = 0L;
    int nrOfElements = 1000;

    public ComputationStaticDestination(DefaultPGroup group) {
        this.parallelGroup = group;
    }

    @Override
    public void onMessage(Integer msg) {
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
