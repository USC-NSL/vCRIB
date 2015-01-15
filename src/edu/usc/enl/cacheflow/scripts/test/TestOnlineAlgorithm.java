package edu.usc.enl.cacheflow.scripts.test;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.NewNeighborReplacement;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.flow.classifier.LinearMatchTrafficProcessor;
import edu.usc.enl.cacheflow.processor.flow.classifier.TwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.flow.online.FlowMovementProcessor;
import edu.usc.enl.cacheflow.processor.network.RunFlowsOnNetworkProcessor2;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/12/12
 * Time: 8:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestOnlineAlgorithm {
    public static void main(String[] args) {


        final String topologyFile = "input/hotcloud/topology/tree_20000_60000.txt";
        final String flowFile = "input/hotcloud/flows/true_true_5_50/conclassbench_4096_1.txt_trace";
        final String assignmentFile = "input/osdi/testassignment.txt";
        final String partitionFile = "input/hotcloud/partition/20000/20000_classbench_4096_1.txt";
        Map<String, Object> parameters = new HashMap<String, Object>();
        try {
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.createDefaultAggregator(), new HashSet<Rule>()), topologyFile, parameters, new LinkedList<Topology>()).get(0);
            Collection<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology), flowFile, parameters, new LinkedList<Flow>());

            List<Partition> partitions = Util.loadFile(new PartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()), partitionFile, parameters, new LinkedList<Partition>());
            Map<Partition, Map<Rule, Collection<Flow>>> matchedFlow =
                    new TwoLevelTrafficProcessor(new LinearMatchTrafficProcessor(), new LinearMatchTrafficProcessor()).classify(flows, partitions);

            final Assignment assignment = Util.loadFile(new AssignmentFactory(new FileFactory.EndOfFileCondition(), topology, partitions)
                    , assignmentFile, parameters, new LinkedList<Assignment>()).get(0);

            //load the initial state
            long startTime = System.currentTimeMillis();
            {
                //for each assignment I need to add forwarding rule and run traffic
                //new SetForwardingRulesProcessor().process(assignment, matchedFlow, topology); //TODO needs forwarding rules
                new RunFlowsOnNetworkProcessor2().process(topology, flows);

                Statistics stats = topology.getStat(new HashMap<String, Object>());
                System.out.println(stats.getStat(Topology.TRAFFIC_STAT));
                long initTime = System.currentTimeMillis();
                System.out.println("time: " + (initTime - startTime) / 1000);
                startTime = initTime;

            }
            {
                //move flows
                flows = new FlowMovementProcessor(
                        Util.random,
                        0.5, true).process(topology, flows);

                //flows has changed need to rematch them
                matchedFlow = new TwoLevelTrafficProcessor(new LinearMatchTrafficProcessor(), new LinearMatchTrafficProcessor()).classify(flows, partitions);

                //update forwarding rules and run new flows on the network                                
                //new SetForwardingRulesProcessor().process(assignment, matchedFlow, topology);      //TODO needs forwarding rules
                new RunFlowsOnNetworkProcessor2().process(topology, flows);

                Statistics stats = topology.getStat(new HashMap<String, Object>());
                System.out.println(stats.getStat(Topology.TRAFFIC_STAT));
                long moveTime = System.currentTimeMillis();
                System.out.println("time: " + (moveTime - startTime) / 1000);
                startTime = moveTime;
            }

            //replace flows
            // it does change forwarding rules and flows
            {

                final Assignment newAssignment = new Assignment(new NewNeighborReplacement().replace(assignment.getPlacement(), matchedFlow, topology));
                Statistics stats = topology.getStat(new HashMap<String, Object>());
                System.out.println(stats.getStat(Topology.TRAFFIC_STAT));
                long onlineTime = System.currentTimeMillis();
                System.out.println("time: " + (onlineTime - startTime));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        /*final Processor<?, List<Flow>> flowSizeChangeProcessor = new FlowSizeChangeProcessor(
                topologyProcessor,
                Util.random,
                flowMovementProcessor,
                0.1,
                0.2);*/

    }
}
