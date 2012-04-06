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

import groovyx.gpars.dataflow.Dataflow
import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowVariable

public class FlowGraphUnfairActorTest extends GroovyTestCase {

    public void testFlowGraphSingleOperator() {
        println("\n\ntestFlowGraphSingleOperator")

        final DataflowVariable a = new DataflowVariable()
        final DataflowVariable b = new DataflowVariable()
        final DataflowQueue c = new DataflowQueue()
        final DataflowVariable d = new DataflowVariable()
        final DataflowQueue e = new DataflowQueue()

        FlowGraph fGraph = new FlowGraph()
        def op = fGraph.operator([a, b, c], [d, e]) {x, y, z ->
            bindOutput 0, x + y + z
            bindOutput 1, x * y * z
        }

        a << 5
        b << 20
        c << 40

        fGraph.waitForAll()

        assert 65 == d.val
        assert 4000 == e.val
    }

    public void testFlowGraphOperatorWithDoubleWaitOnChannel() {
        println("\n\ntestFlowGraphOperatorWithDoubleWaitOnChannel")

        final DataflowQueue a = new DataflowQueue()
        final DataflowQueue b = new DataflowQueue()

        FlowGraph fGraph = new FlowGraph()

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
        println("\n\ntestFlowGraphNonCommutativeOperator")

        final DataflowQueue a = new DataflowQueue()
        final DataflowQueue b = new DataflowQueue()
        final DataflowQueue c = new DataflowQueue()

        FlowGraph fGraph = new FlowGraph()

        def op = fGraph.operator([a, b], [c]) {x, y ->
            bindOutput 0, 2 * x + y
        }

        Dataflow.task { a << 5 }
        Dataflow.task { b << 20 }

        fGraph.waitForAll()

        assert 30 == c.val
    }

    public void testFlowGraphSimpleOperators() {
        println("\n\ntestFlowGraphSimpleOperators")

        final DataflowQueue a = new DataflowQueue()
        final DataflowQueue b = new DataflowQueue()
        final DataflowQueue c = new DataflowQueue()
        a << 1
        a << 2
        a << 3
        a << 4
        a << 5

        FlowGraph fGraph = new FlowGraph()

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
        println("\n\ntestFlowGraphExternalOutputs")

        final DataflowChannel channel1 = new DataflowQueue()
        final DataflowChannel channel2 = new DataflowQueue()
        final DataflowChannel channel3 = new DataflowQueue()
        final List<Integer> outputs = new ArrayList<Integer>()

        FlowGraph fGraph = new FlowGraph()

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
        println("\n\ntestFlowGraphBranchingGraph")

        final DataflowChannel channel1 = new DataflowQueue()
        final DataflowChannel channel2 = new DataflowQueue()

        final DataflowChannel branch1 = new DataflowQueue()
        final DataflowChannel branch2 = new DataflowQueue()

        final List<Integer> outputs = new ArrayList<Integer>()

        FlowGraph fGraph = new FlowGraph()

        final DataflowProcessor op1 = fGraph.operator([channel1], [channel2]) { input ->
            bindOutput input * input
        }

        final List branchStreams = Arrays.asList(branch1, branch2)

        final DataflowProcessor op2 = fGraph.operator([channel2], branchStreams) { input ->
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

    public void testFlowGraphLongLinkedOperators() {
        println("\n\ntestFlowGraphLongLinkedOperators")

        int LIMIT = 20; // Number of channels

        List<DataflowChannel> channels = new ArrayList<DataflowChannel>()

        FlowGraph fGraph = new FlowGraph()

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
        println("\n\ntestFlowGraphLongRingOperators")

        int LIMIT = 20; // Number of channels
        int TIMES_AROUND_RING = 3;

        List<DataflowChannel> channels = new ArrayList<DataflowChannel>()

        FlowGraph fGraph = new FlowGraph()

        for (int i = 0; i < LIMIT; i++)
            channels.add(new DataflowQueue())

        for (int i = 0; i < LIMIT; i++) {
            DataflowChannel input = channels.get(i)
            DataflowChannel output = channels.get((i + 1) % LIMIT)

            int counter = 0; // Close over this variable
            fGraph.operator([input], [output]) { value ->
                if (counter <= TIMES_AROUND_RING) {
                    bindOutput counter
                    counter++
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
    }

}
