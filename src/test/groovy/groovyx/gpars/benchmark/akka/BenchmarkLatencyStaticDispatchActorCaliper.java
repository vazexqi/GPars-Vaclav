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
import com.google.caliper.api.Benchmark;
import com.google.caliper.api.VmParam;
import com.google.caliper.runner.CaliperMain;
import groovyx.gpars.actor.Actor;
import groovyx.gpars.actor.StaticDispatchActor;
import groovyx.gpars.group.DefaultPGroup;
import groovyx.gpars.group.PGroup;
import groovyx.gpars.scheduler.FJPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;


public class BenchmarkLatencyStaticDispatchActorCaliper extends Benchmark {
    @Param({"1", "2", "4"}) int numberOfClients;
    @VmParam({"-server"}) String server;
    @VmParam({"-Xms512M"}) String xms;
    @VmParam({"-Xmx1024M"}) String xmx;
    @VmParam({"-XX:+UseParallelGC"}) String gc;

    int repeatNum = 200*500;  //Value used by Akka
    final int maxClients = 4;       //Value used by Akka
    int repeatsPerClient;
    PGroup group;
    CountDownLatch cdl;
    List<Actor> clients;
    long total_duration;
    int total_count;




    private void setup(){

        total_duration=0;
        total_count =0;
        group = new DefaultPGroup(new FJPool(maxClients));
        cdl = new CountDownLatch(numberOfClients);
        repeatsPerClient = repeatNum/numberOfClients;
        clients = new ArrayList<Actor>();

        for(int i=0; i < numberOfClients; i++){
            Actor destination = new LatencyStaticDestination(group).start();
            Actor w4 = new LatencyStaticWayPoint(destination, group).start();
            Actor w3 = new LatencyStaticWayPoint(w4, group).start();
            Actor w2 = new LatencyStaticWayPoint(w3, group).start();
            Actor w1 = new LatencyStaticWayPoint(w2, group).start();
            clients.add(new LatencyStaticClient(w1, cdl, repeatsPerClient, group, this));
        }
    }

    private void teardown(){
        for(Actor client: clients){
            client.send(new LatencyStaticMessage(0, null, "POISON"));
        }
        for(Actor client: clients){
            try {
                client.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        group.shutdown();
    }

    public synchronized void add_duration(long duration){
        total_duration += duration;
        total_count++;
    }

    public int totalMessages(){
        return repeatNum;
    }

    public long latencyStaticDispatchActorLatency(int dummy){
        setup();
        for(Actor client: clients){
            client.start();
            client.send(new LatencyStaticMessage(0,null,"RUN"));
        }

        try {
            cdl.await(); //differ
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        teardown();

        return total_duration;
    }

    public static void main(String [] args){
        CaliperMain.main(BenchmarkLatencyStaticDispatchActorCaliper.class, args);
    }
}

class LatencyStaticMessage {
    final long sendTime;
    final Actor sender;
    String msg;

    LatencyStaticMessage(final long sendTime, final Actor sender, String msg){
        this.sendTime = sendTime;
        this.sender = sender;
        this.msg = msg;
    }

    public Actor sender(){
        return sender;
    }
}

class LatencyStaticWayPoint extends StaticDispatchActor<LatencyStaticMessage> {
    final Actor next;

    LatencyStaticWayPoint(final Actor next, PGroup group){
        this.next = next;
        this.parallelGroup = group;
       // this.makeFair();
    }

    @Override
    public void onMessage(LatencyStaticMessage msg){
        if(msg.msg.equals("MESSAGE")){
            next.send(msg);
        }
        else if(msg.msg.equals("POISON")){
            next.send(msg);
            this.terminate();
        }
    }

}

class LatencyStaticDestination extends StaticDispatchActor<LatencyStaticMessage>{
    LatencyStaticDestination(PGroup group){
        //this.makeFair();
        this.parallelGroup = group;
    }
    @Override
    public void onMessage(LatencyStaticMessage msg){
        if(msg.msg.equals("MESSAGE")){
            msg.sender().send( msg );
        }
        else if(msg.msg.equals("POISON")){
            this.terminate();
        }
    }

}

class LatencyStaticClient extends StaticDispatchActor<LatencyStaticMessage>{
    long sent = 0L;
    long received = 0L;
    final Actor next;
    CountDownLatch latch;
    final int repeat;
    final BenchmarkLatencyStaticDispatchActorCaliper benchmark;

    LatencyStaticClient(final Actor next, CountDownLatch latch, final int repeat, PGroup group, BenchmarkLatencyStaticDispatchActorCaliper benchmark){
        this.next = next;
        this.latch = latch;
        this.repeat = repeat;
        this.parallelGroup = group;
        this.benchmark = benchmark;
        //this.makeFair();
    }

    void shortDelay(int micros, long n) {
        if (micros > 0) {
            int sampling = 1000 / micros;
            if ((n % sampling) == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void onMessage(LatencyStaticMessage msg){
        if(msg.msg.equals("MESSAGE")){
            long duration = System.nanoTime() - msg.sendTime;
            benchmark.add_duration(duration);
            received++;
            if (sent < repeat){
                shortDelay(250, received);  // Value used by Akka
                next.send( new LatencyStaticMessage(System.nanoTime(), this, "MESSAGE"));
                sent++;
            } else if (received >= repeat){
                latch.countDown();
            }
        }

        else if(msg.msg.equals("RUN")){
            int initialDelay = new Random(0).nextInt(20);    //Value used by Akka
            try {
                Thread.sleep(initialDelay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            next.send( new LatencyStaticMessage(System.nanoTime(), this, "MESSAGE"));
            sent++;
        }

        else if(msg.msg.equals("POISON")){
            next.send(msg);
            terminate();
        }
    }

}