// GPars - Groovy Parallel Systems
//
// Copyright © 2008-11  The original author or authors
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

package groovyx.gpars.benchmark;

import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import groovyx.gpars.DataflowMessagingRunnable;
import groovyx.gpars.dataflow.DataflowQueue;
import groovyx.gpars.dataflow.operator.DataflowProcessor;
import groovyx.gpars.dataflow.operator.FlowGraph;
import groovyx.gpars.dataflow.operator.PoisonPill;
import groovyx.gpars.group.DefaultPGroup;
import groovyx.gpars.scheduler.FJPool;

import java.util.Arrays;

public class BenchmarkFlowGraphOperator extends SimpleBenchmark {

    final int concurrencyLevel = 8;
    private DataflowQueue queue;
    private static final int LIMIT = 2000000;
    private int sum;
    private DefaultPGroup pGroup;

    public void timeOriginalOperator(int reps) throws InterruptedException {
        for (int rep = 0; rep < reps; rep++) {

            pGroup = new DefaultPGroup(new FJPool(concurrencyLevel));

            queue = new DataflowQueue();
            for (int i = 1; i <= LIMIT; i++) {
                queue.bind(i);
            }
            queue.bind(-1);
            queue.bind(PoisonPill.getInstance());

            sum = 0;

            DataflowProcessor op = pGroup.operator(Arrays.asList(queue), Arrays.asList(), new DataflowMessagingRunnable(1) {
                @Override
                protected void doRun(final Object... arguments) {
                    int it = (Integer) arguments[0];
                    if (it == -1)
                        getOwningProcessor().terminate();
                    else
                        sum += it;
                }
            });
            op.join();
            pGroup.shutdown();
        }
    }

    public void timeFlowGraphOperator(int reps) {
        for (int rep = 0; rep < reps; rep++) {
            queue = new DataflowQueue();
            for (int i = 1; i <= LIMIT; i++) {
                queue.bind(i);
            }
            queue.bind(-1); // Just to be consistent

            sum = 0;

            FlowGraph fGraph = new FlowGraph(concurrencyLevel);
            fGraph.operator(Arrays.asList(queue), Arrays.asList(), new DataflowMessagingRunnable(1) {
                @Override
                protected void doRun(final Object... arguments) {
                    int it = (Integer) arguments[0];
                    sum += it;
                }
            });
            fGraph.waitForAll();
        }
    }

    // For testing with caliper
    public static void main(String[] args) throws Exception {
        Runner.main(BenchmarkFlowGraphOperator.class, args);
    }
}
