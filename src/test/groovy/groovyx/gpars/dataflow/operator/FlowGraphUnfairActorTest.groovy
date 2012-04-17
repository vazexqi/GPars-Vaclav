// GPars - Groovy Parallel Systems
//
// Copyright © 2008-11  The original author or authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

package groovyx.gpars.dataflow.operator

import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

import java.util.concurrent.atomic.AtomicInteger

/**
 * There are two categories of tests: normal tests and inverse tests. Normal tests check to see that when we use
 * FlowGraph's waitForAll(), we get the right results. Inverse tests check to see that when we <b>don't</b> use
 * FlowGraph's waitForAll() that we don't get the desired results. This ensures that FlowGraph is responsible for both
 * generating good behavior and also avoiding bad behavior.
 */
public class FlowGraphUnfairActorTest extends GroovyTestCase {

    protected FlowGraph createFlowGraphInstance() {
        return new FlowGraph()
    }

    public void testFlowGraphSingleOperator() {
//        println("\n\ntestFlowGraphSingleOperator")

        final DataflowVariable a = new DataflowVariable()
        final DataflowVariable b = new DataflowVariable()
        final DataflowQueue c = new DataflowQueue()
        final DataflowVariable d = new DataflowVariable()
        final DataflowQueue e = new DataflowQueue()

        FlowGraph fGraph = createFlowGraphInstance()
        def op = fGraph.operator([a, b, c], [d, e]) {x, y, z ->
            bindOutput 0, x + y + z
            bindOutput 1, x * y * z
        }

        a << 5
        b << 20
        c << 40

        fGraph.waitForAll()

        // It is more important that the values are bound by this point.
        // Using getVal() to compare the values is wrong because getVal() will block and wait until it's ready
        // Everything must be *done* after waitForAll()
        assert d.isBound()
        assert e.isBound()
    }

    public void testInverseFlowGraphSingleOperator() {
//        println("\n\ntestFlowGraphSingleOperator")

        final DataflowVariable a = new DataflowVariable()
        final DataflowVariable b = new DataflowVariable()
        final DataflowQueue c = new DataflowQueue()
        final DataflowVariable d = new DataflowVariable()
        final DataflowQueue e = new DataflowQueue()

        FlowGraph fGraph = createFlowGraphInstance()
        def op = fGraph.operator([a, b, c], [d, e]) {x, y, z ->
            bindOutput 0, x + y + z
            bindOutput 1, x * y * z
        }

        a << 5
        b << 20
        c << 40

        // Without this statement the values should not be bound yet
        // fGraph.waitForAll()

        // It is more important that the values are bound by this point.
        // Using getVal() to compare the values is wrong because getVal() will block and wait until it's ready
        // Everything must be *done* after waitForAll()
        assert !d.isBound()
        assert !e.isBound()
    }

    public void testFlowGraphOperatorWithDoubleWaitOnChannel() {
//        println("\n\ntestFlowGraphOperatorWithDoubleWaitOnChannel")

        final DataflowQueue a = new DataflowQueue()
        final DataflowQueue b = new DataflowQueue()

        FlowGraph fGraph = createFlowGraphInstance()

        def op = fGraph.operator([a, a], [b]) {x, y ->
            bindOutput 0, x + y
        }

        a << 1
        a << 2
        a << 3
        a << 4

        fGraph.waitForAll()

        assert 3 == b.val
        assert 7 == b.val
    }

    public void testFlowGraphNonCommutativeOperator() {
//        println("\n\ntestFlowGraphNonCommutativeOperator")

        final DataflowQueue a = new DataflowQueue()
        final DataflowQueue b = new DataflowQueue()
        final DataflowVariable c = new DataflowVariable()

        FlowGraph fGraph = createFlowGraphInstance()

        def op = fGraph.operator([a, b], [c]) {x, y ->
            sleep(500) // deliberate delay
            bindOutput 0, 2 * x + y
        }

        a << 5
        b << 20

        fGraph.waitForAll()

        assert c.isBound()
    }

    public void testInverseFlowGraphNonCommutativeOperator() {
//        println("\n\ntestInverseFlowGraphNonCommutativeOperator")

        final DataflowQueue a = new DataflowQueue()
        final DataflowQueue b = new DataflowQueue()
        final DataflowVariable c = new DataflowVariable()

        FlowGraph fGraph = createFlowGraphInstance()

        def op = fGraph.operator([a, b], [c]) {x, y ->
            sleep(500) // deliberate delay
            bindOutput 0, 2 * x + y
        }

        a << 5
        b << 20

        // fGraph.waitForAll()

        assert !c.isBound()
    }

    public void testFlowGraphSimpleOperators() {
//        println("\n\ntestFlowGraphSimpleOperators")

        final DataflowQueue a = new DataflowQueue()
        final DataflowQueue b = new DataflowQueue()
        final DataflowQueue c = new DataflowQueue()
        a << 1
        a << 2
        a << 3
        a << 4
        a << 5

        FlowGraph fGraph = createFlowGraphInstance()

        def op1 = fGraph.operator([a], [b]) {v ->
            bindOutput 2 * v
        }

        def op2 = fGraph.operator([b], [c]) {v ->
            bindOutput v + 1
        }

        fGraph.waitForAll()

        assert 3 == c.val
        assert 5 == c.val
        assert 7 == c.val
        assert 9 == c.val
        assert 11 == c.val
    }

    public void testFlowGraphExternalOutputs() {
//        println("\n\ntestFlowGraphExternalOutputs")

        final DataflowChannel channel1 = new DataflowQueue()
        final DataflowChannel channel2 = new DataflowQueue()
        final DataflowChannel channel3 = new DataflowQueue()
        final List<Integer> outputs = new ArrayList<Integer>()

        FlowGraph fGraph = createFlowGraphInstance()

        final DataflowProcessor operator1 = fGraph.operator([channel1], [channel2]) { input ->
            bindOutput input * input
        }

        final DataflowProcessor operator2 = fGraph.operator([channel2], [channel3]) { input ->
            bindOutput input - 1
        }

        final DataflowProcessor operator3 = fGraph.operator([channel3], []) { input ->
            outputs.add(input)
        }

        channel1 << 1
        channel2 << 2
        channel2 << 3
        channel3 << 4

        fGraph.waitForAll()

        assertEquals("Output size does not match", 4, outputs.size())
    }

    public void testFlowGraphBranchingGraph() {
//        println("\n\ntestFlowGraphBranchingGraph")

        final DataflowChannel channel1 = new DataflowQueue()
        final DataflowChannel channel2 = new DataflowQueue()

        final DataflowChannel branch1 = new DataflowQueue()
        final DataflowChannel branch2 = new DataflowQueue()

        final List<Integer> outputs = new ArrayList<Integer>()

        FlowGraph fGraph = createFlowGraphInstance()

        final DataflowProcessor op1 = fGraph.operator([channel1], [channel2]) { input ->
            bindOutput input * input
        }

        final DataflowProcessor op2 = fGraph.operator([channel2], [branch1, branch2]) { input ->
            bindAllOutputsAtomically input
        }

        final DataflowProcessor op3 = fGraph.operator([branch1], []) { input ->
            outputs.add(input * 2)
        }

        final DataflowProcessor op4 = fGraph.operator([branch2], []) { input ->
            outputs.add(input / 2)
        }

        channel1.bind(1)
        channel1.bind(2)
        channel1.bind(3)
        channel1.bind(4)

        fGraph.waitForAll()

        assertEquals("Output size does not match", 8, outputs.size())
    }

    public void testFlowGraphForkingOperators() {
//        println("\n\ntestFlowGraphForkingOperator")

        final DataflowQueue a = new DataflowQueue()
        final DataflowQueue b = new DataflowQueue()
        final DataflowQueue c = new DataflowQueue()
        final DataflowQueue d = new DataflowQueue()

        FlowGraph fGraph = createFlowGraphInstance()

        def op1 = fGraph.operator([a], [b, c, d], 5) {x ->
            bindAllOutputs x
        }

        final IntRange range = 1..100
        range.each {a << it}

        fGraph.waitForAll()

        def bs = range.collect {b.val}
        def cs = range.collect {c.val}
        def ds = range.collect {d.val}
        assert bs.size() == range.to
        assert cs.size() == range.to
        assert ds.size() == range.to
    }

    public void testFlowGraphLongLinkedOperators() {
//        println("\n\ntestFlowGraphLongLinkedOperators")

        int LIMIT = 20; // Number of channels

        List<DataflowChannel> channels = new ArrayList<DataflowChannel>()

        FlowGraph fGraph = createFlowGraphInstance()

        channels.add(new DataflowQueue()) // First channel

        for (int i = 0; i < LIMIT; i++) {
            channels.add(new DataflowQueue())

            DataflowProcessor op = fGraph.operator([channels.get(i)], [channels.get(i + 1)]) {input ->
                bindOutput input
            }
        }

        channels.get(0) << 1

        // This is the topology
        // --ch1--> op1 --ch2--> op2 --ch3 --> op3 --ch4-->

        fGraph.waitForAll()

        assert channels.get(LIMIT).getVal() == 1;
    }

    public void testFlowGraphLongRingOperators() {
//        println("\n\ntestFlowGraphLongRingOperators")

        int LIMIT = 50 // Number of channels
        int TIMES_AROUND_RING = 3
        AtomicInteger globalCounter = new AtomicInteger()

        List<DataflowChannel> channels = new ArrayList<DataflowChannel>()

        FlowGraph fGraph = createFlowGraphInstance()

        for (int i = 0; i < LIMIT; i++)
            channels.add(new DataflowQueue())

        for (int i = 0; i < LIMIT; i++) {
            DataflowChannel input = channels.get(i)
            DataflowChannel output = channels.get((i + 1) % LIMIT)

            int localCounter = 0; // Close over this variable
            fGraph.operator([input], [output]) { value ->
                if (localCounter < TIMES_AROUND_RING) {
                    bindOutput localCounter
                    localCounter++
                    globalCounter.incrementAndGet()
                }
            }
        }

        channels.get(0) << 1

        // This is the topology
        //   ---op1 ---
        //  |          |
        //  ch0       ch1
        //  |          |
        //   ---op2 ---
        //

        fGraph.waitForAll()
        assert globalCounter.get() == LIMIT * TIMES_AROUND_RING
    }

    public void testFlowGraphCompletedBeforeWaitForAll() {
//        println("\n\ntestFlowGraphCompletedBeforeWaitForAll")

        final DataflowVariable a = new DataflowVariable()
        final DataflowVariable b = new DataflowVariable()
        final DataflowQueue c = new DataflowQueue()
        final DataflowVariable d = new DataflowVariable()
        final DataflowQueue e = new DataflowQueue()

        FlowGraph fGraph = createFlowGraphInstance()
        def op = fGraph.operator([a, b, c], [d, e]) {x, y, z ->
            bindOutput 0, x + y + z
            bindOutput 1, x * y * z
        }

        a << 5
        b << 20
        c << 40

        sleep(1000)

        fGraph.waitForAll()

        assert 65 == d.val
        assert 4000 == e.val

    }

}
