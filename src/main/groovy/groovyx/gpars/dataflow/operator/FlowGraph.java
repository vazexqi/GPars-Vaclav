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

package groovyx.gpars.dataflow.operator;

import groovy.lang.Closure;
import groovyx.gpars.group.DefaultPGroup;
import groovyx.gpars.group.PGroup;
import groovyx.gpars.scheduler.FJPool;
import jsr166y.CountedCompleter;
import jsr166y.ForkJoinPool;
import org.codehaus.groovy.runtime.InvokerInvocationException;
import org.codehaus.groovy.runtime.NullObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FlowGraph {
    private final List<DataflowProcessor> processors;
    private final PGroup pGroup;
    private boolean isFair;
    private CountedCompleter graph;
    private ForkJoinPool forkJoinPool;

    /**
     * Creates a new FlowGraph with a ForkJoinPool as its ThreadPool. All FlowGraphs use the ForkJoinPool to leverage
     * its facility for CountedCompleter.
     */
    public FlowGraph() {
        processors = new ArrayList<DataflowProcessor>();

        FJPool pool = new FJPool();
        forkJoinPool = pool.getForkJoinPool();
        pGroup = new DefaultPGroup(pool);
        graph = new CountedCompleter() {
            @Override
            public void compute() {
                tryComplete();
            }
        };
    }

    /**
     * Creates a new FlowGraph with a ForkJoinPool as its ThreadPool. All FlowGraphs use the ForkJoinPool to leverage
     * its facility for CountedCompleter. This version of the constructor makes all DataflowActors created through the
     * FlowGraph, behave fairly.
     */
    public FlowGraph(boolean isFair) {
        this();
        this.isFair = isFair;
    }

    /**
     * This is deeply linked to the internals of the DataflowProcessorActor. The terminating condition is the pair (No
     * Active Processors, No More Messages). Because of the way that DataflowProcessorActors work, it is sufficient to
     * check first that all actors are no longer active. Then we check the number of messages left. If the number of
     * messages left is greater than zero, the semantics of DataflowProcessorActor means that <i>eventually</i> at least
     * one actor will wake up to process the message.
     * <p/>
     * In general, this assumption is not true of all actor systems. Some actor systems (even the Actor based class in
     * GPars) can leave messages in the mailbox even when they are technically done already. In those cases, it is not
     * possible to use this method for checking termination.
     *
     * @throws InterruptedException
     */
    public void waitForAll() {
        forkJoinPool.invoke(graph);
        terminateProcessors();
        pGroup.shutdown();

    }

    /**
     * Terminating processors works by sending messages. This will flood the system with new
     * groovyx.gpars.actor.Actor#TERMINATE_MESSAGE but we don't care anymore at this point about new messages in the
     * system.
     */
    private void terminateProcessors() {
        for (DataflowProcessor processor : processors) {
            processor.terminate();
        }
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // DataflowOperator
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DataflowProcessor operator(final List inputChannels, final List outputChannels, final Closure code) {
        final HashMap<String, List> params = new HashMap<String, List>(5);
        params.put(DataflowProcessor.INPUTS, inputChannels);
        params.put(DataflowProcessor.OUTPUTS, outputChannels);

        final DataflowOperator operator = new DataflowOperator(pGroup, params, code);
        substituteOperatorCore(operator);

        if (isFair) operator.actor.makeFair();

        processors.add(operator);
        return operator.start();
    }

    public DataflowProcessor operator(final List inputChannels, final List outputChannels, final int maxForks, final Closure code) {
        final HashMap<String, Object> params = new HashMap<String, Object>(5);
        params.put(DataflowProcessor.INPUTS, inputChannels);
        params.put(DataflowProcessor.OUTPUTS, outputChannels);
        params.put(DataflowProcessor.MAX_FORKS, maxForks);

        final DataflowOperator operator = new DataflowOperator(pGroup, params, code);
        substituteOperatorCore(operator);

        if (isFair) operator.actor.makeFair();

        processors.add(operator);
        return operator.start();
    }

    // A rather hackish way to reimplement the innards of the Actor's messaging core
    private void substituteOperatorCore(final DataflowOperator operator) {
        final DataflowProcessorActor processorActor = operator.actor;
        final Closure code = processorActor.getCode();

        processorActor.setCore(processorActor.new ActorAsyncMessagingCore(code) {

            @Override
            public void store(final Object message) {
                queue.add(message != null ? message : NullObject.getNullObject());
                graph.addToPendingCount(1);
                if (activeUpdater.compareAndSet(this, PASSIVE, ACTIVE)) {
                    graph.addToPendingCount(1);
                    threadPool.execute(this);
                }
            }

            @Override
            protected void schedule() {
                if (!queue.isEmpty() && activeUpdater.compareAndSet(this, PASSIVE, ACTIVE)) {
                    graph.addToPendingCount(1);
                    threadPool.execute(this);
                }
            }

            @Override
            public Object sweepNextMessage() {
                return queue.poll();
            }

            @Override
            public void run() {
                try {
                    threadAssigned();
                    if (!continueProcessingMessages()) return;
                    Object message = queue.poll();
                    while (message != null) {
                        handleMessage(message);
                        graph.tryComplete();
                        if (Thread.interrupted()) throw new InterruptedException();
                        if (isFair() || !continueProcessingMessages()) break;
                        message = queue.poll();
                    }
                } catch (InvokerInvocationException e) {
                    registerError(e.getCause());
                } catch (Exception e) {
                    registerError(e);
                } finally {
                    threadUnassigned();
                    activeUpdater.set(this, PASSIVE);
                    graph.tryComplete();
                    if (continueProcessingMessages()) schedule();
                }
            }
        });
    }
}
