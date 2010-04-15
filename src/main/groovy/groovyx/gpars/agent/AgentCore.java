// GPars (formerly GParallelizer)
//
// Copyright © 2008-10  The original author or authors
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

package groovyx.gpars.agent;

import groovyx.gpars.util.PoolUtils;
import org.codehaus.groovy.runtime.NullObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author Vaclav Pech
 *         Date: 13.4.2010
 */
public abstract class AgentCore implements Runnable {

    /**
     * A thread pool shared by all agents
     */
    private static final ExecutorService pool = Executors.newFixedThreadPool(PoolUtils.retrieveDefaultPoolSize(), new AgentThreadFactory());

    /**
     * The thread pool to use with this agent
     */
    private ExecutorService threadPool = pool;

    /**
     * Retrieves the thread pool used by the agent
     *
     * @return The thread pool
     */
    public final ExecutorService getThreadPool() {
        return threadPool;
    }

    /**
     * Sets a new thread pool to be used by the agent
     *
     * @param threadPool The thread pool to use
     */
    public final void attachToThreadPool(final ExecutorService threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * Holds agent errors
     */
    private List<Exception> errors;

    /**
     * Incoming messages
     */
    private final Queue<Object> queue = new ConcurrentLinkedQueue<Object>();

    /**
     * Indicates, whether there's an active thread handling a message inside the agent's body
     */
    private volatile int active = PASSIVE;
    private static final AtomicIntegerFieldUpdater<AgentCore> activeUpdater = AtomicIntegerFieldUpdater.newUpdater(AgentCore.class, "active");
    private static final int PASSIVE = 0;
    private static final int ACTIVE = 1;

    /**
     * Adds the message to the agent\s message queue
     *
     * @param message A value or a closure
     */
    public final void send(final Object message) {
        queue.add(message != null ? message : NullObject.getNullObject());
        schedule();
    }

    /**
     * Adds the message to the agent\s message queue
     *
     * @param message A value or a closure
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public final void leftShift(final Object message) {
        send(message);
    }

    /**
     * Dynamically dispatches the method call
     *
     * @param message A value or a closure
     */
    abstract void handleMessage(final Object message);

    /**
     * Schedules processing of a next message, if there are some and if there isn't an active thread handling a message at the moment
     */
    void schedule() {
        if (!queue.isEmpty() && activeUpdater.compareAndSet(this, PASSIVE, ACTIVE)) {
            threadPool.submit(this);
        }
    }

    /**
     * Handles a single message from the message queue
     */
    @SuppressWarnings({"CatchGenericClass"})
    public void run() {
        try {
            final Object message = queue.poll();
            if (message != null) this.handleMessage(message);
        } catch (Exception e) {
            registerError(e);
        } finally {
            activeUpdater.set(this, PASSIVE);
            schedule();
        }
    }

    /**
     * Adds the exception to the list of thrown exceptions
     *
     * @param e The exception to store
     */
    @SuppressWarnings({"MethodOnlyUsedFromInnerClass", "SynchronizedMethod"})
    private synchronized void registerError(final Exception e) {
        if (errors == null) errors = new ArrayList<Exception>();
        errors.add(e);
    }

    /**
     * Retrieves a list of exception thrown within the agent's body.
     * Clears the exception history
     *
     * @return A detached collection of exception that have occurred in the agent's body
     */
    @SuppressWarnings({"SynchronizedMethod", "ReturnOfCollectionOrArrayField"})
    public synchronized List<Exception> getErrors() {
        if (errors == null) return Collections.emptyList();
        try {
            return errors;
        } finally {
            errors = null;
        }
    }
}
