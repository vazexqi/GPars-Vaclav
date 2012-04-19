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
import groovyx.gpars.scheduler.Pool;
import groovyx.gpars.util.PoolUtils;
import jsr166y.CountedCompleter;
import jsr166y.ForkJoinPool;
import org.codehaus.groovy.runtime.InvokerInvocationException;
import org.codehaus.groovy.runtime.NullObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

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

        FJPool pool = new FJPool(FJPool.createForkJoinAsyncModePool(PoolUtils.retrieveDefaultPoolSize()));
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

    public CountedCompleter getGraph() {
        return graph;
    }

    //TODO: Using this requires some caution since it seems to cause some race condition on the wait
    public void incrementWaitCount() {
        getGraph().addToPendingCount(1);
    }

    public void decrementWaitCount() {
        getGraph().tryComplete();
    }

    /**
     * This is deeply linked to the internals of the DataflowProcessorActor. The terminating condition is the pair (No
     * Active Processors, No More Messages). In general, this assumption is not true of all actor systems. Some actor
     * systems (even the Actor based class in GPars) can leave messages in the mailbox even when they are technically
     * done already. In those cases, it is not possible to use this method for checking termination.
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

        final DataflowOperator operator = new FlowGraphDataflowOperator(pGroup, params, code);
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

        final DataflowOperator operator = new FlowGraphDataflowOperator(pGroup, params, code);
        substituteOperatorCore(operator);

        if (isFair) operator.actor.makeFair();

        processors.add(operator);
        return operator.start();
    }

    private class FlowGraphDataflowOperator extends DataflowOperator {

        /**
         * Creates an operator After creation the operator needs to be started using the start() method.
         *
         * @param group    The group the thread pool of which o use
         * @param channels A map specifying "inputs" and "outputs" - dataflow channels (instances of the DataflowQueue
         *                 or DataflowVariable classes) to use for inputs and outputs
         * @param code     The operator's body to run each time all inputs have a value to read
         */
        public FlowGraphDataflowOperator(final PGroup group, final Map channels, final Closure code) {
            super(group, channels, code);
            final int parameters = code.getMaximumNumberOfParameters();
            if (verifyChannelParameters(channels, parameters))
                throw new IllegalArgumentException("The operator's body accepts " + parameters + " parameters while it is given " + countInputChannels(channels) + " input streams. The numbers must match.");
            if (shouldBeMultiThreaded(channels)) {
                checkMaxForks(channels);
                this.actor = new FlowGraphForkingDataflowOperator(this, group, extractOutputs(channels), extractInputs(channels), (Closure) code.clone(), (Integer) channels.get(MAX_FORKS));
            } else {
                this.actor = new DataflowOperatorActor(this, group, extractOutputs(channels), extractInputs(channels), (Closure) code.clone());
            }
        }

    }

    /**
     * This class facilitates running multiple instances of the actor body in parallel (according to maxForks). It
     * mimics the behavior of {@see groovyx.gpars.dataflow.operator.ForkingDataflowOperatorActor} except that it does
     * not use a semaphore to control its concurrency. Acquiring a semaphore is a possibly blocking operation which
     * could deadlock the entire flow graph when run on top of a non-resizable thread pool like ForkJoinPool.
     */
    private class FlowGraphForkingDataflowOperator extends DataflowOperatorActor {
        private final Pool threadPool;
        private final ConcurrentLinkedQueue<List> queue;
        private volatile AtomicInteger currentConcurrency = new AtomicInteger();
        private final int maxForks;

        FlowGraphForkingDataflowOperator(final DataflowOperator owningOperator, final PGroup group, final List outputs, final List inputs, final Closure code, final int maxForks) {
            super(owningOperator, group, outputs, inputs, code);
            this.queue = new ConcurrentLinkedQueue<List>();
            this.threadPool = group.getThreadPool();
            this.maxForks = maxForks;
            this.currentConcurrency.set(1);
        }

        @Override
        void startTask(final List results) {
            if (currentConcurrency.get() <= maxForks) {
                scheduleTask(results);
            } else {
                queue.add(results);
            }
        }

        private void scheduleTask(final List results) {
            currentConcurrency.getAndIncrement();
            graph.addToPendingCount(1);
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        FlowGraphForkingDataflowOperator.super.startTask(results);
                    } finally {
                        currentConcurrency.getAndDecrement();
                        schedulePendingIfAny();
                        graph.tryComplete();
                    }
                }
            });
        }

        private void schedulePendingIfAny() {
            if (currentConcurrency.get() <= maxForks) {
                List result = queue.poll();
                if (result != null) {
                    scheduleTask(result);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // AsyncMessagingCore
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
