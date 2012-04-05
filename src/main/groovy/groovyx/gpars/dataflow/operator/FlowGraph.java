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
import net.jcip.annotations.GuardedBy;
import org.codehaus.groovy.runtime.InvokerInvocationException;
import org.codehaus.groovy.runtime.NullObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FlowGraph {
    private final List<DataflowProcessor> processors;
    private final PGroup pGroup;

    final Lock lock = new ReentrantLock();
    final Condition nonActive = lock.newCondition();

    @GuardedBy("lock")
    private int activeProcessors = 0;

    public FlowGraph() {
        processors = new ArrayList<DataflowProcessor>();
        pGroup = new DefaultPGroup();
    }

    public void decrementActiveProcessors() {
        lock.lock();
        try {
            activeProcessors--;
            assert activeProcessors >= 0;
            if (activeProcessors == 0)
                nonActive.signal();

        } finally {
            lock.unlock();
        }
    }

    public void incrementActiveProcessors() {
        lock.lock();
        try {
            activeProcessors++;
            assert activeProcessors >= 0;
        } finally {
            lock.unlock();
        }
    }

    public void waitForAll() throws InterruptedException {
        lock.lock();
        try {
            while (activeProcessors > 0)
                nonActive.await();
        } finally {
            lock.unlock();
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
        return operator.start();
    }

    public DataflowProcessor operator(final List inputChannels, final List outputChannels, final int maxForks, final Closure code) {
        final HashMap<String, Object> params = new HashMap<String, Object>(5);
        params.put(DataflowProcessor.INPUTS, inputChannels);
        params.put(DataflowProcessor.OUTPUTS, outputChannels);
        params.put(DataflowProcessor.MAX_FORKS, maxForks);

        final DataflowOperator operator = new DataflowOperator(pGroup, params, code);
        substituteOperatorCore(operator);
        return operator.start();
    }

    // A rather hackish way to reimplement the innards of the Actor's messaging core
    private void substituteOperatorCore(final DataflowOperator operator) {
        final DataflowProcessorActor processorActor = operator.actor;
        final Closure code = processorActor.getCode();

        processorActor.setCore(processorActor.new ActorAsyncMessagingCore(code) {

            void becomeActive() {
                System.err.println(processorActor + " became active");
                incrementActiveProcessors();
            }

            void becomePassive() {
                System.err.println(processorActor + " became passive");
                decrementActiveProcessors();
            }

            @Override
            public void store(final Object message) {
                queue.add(message != null ? message : NullObject.getNullObject());
                if (activeUpdater.compareAndSet(this, PASSIVE, ACTIVE)) {
                    becomeActive();
                    threadPool.execute(this);
                }
            }

            @Override
            protected void schedule() {
                if (!queue.isEmpty() && activeUpdater.compareAndSet(this, PASSIVE, ACTIVE)) {
                    becomeActive();
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
                    becomePassive();
                    if (continueProcessingMessages()) schedule();
                }
            }
        });
    }
}
