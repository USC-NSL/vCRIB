package edu.usc.enl.cacheflow.scripts.test;

import edu.usc.enl.cacheflow.algorithms.placement.Assigner;
import edu.usc.enl.cacheflow.algorithms.placement.partitionselection.MaxSizePartitionSorter;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.MinTrafficSwitchSelection;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.LinearMatchTrafficProcessor;
import edu.usc.enl.cacheflow.processor.flow.classifier.PartitionClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.TwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.network.RunFlowsOnNetworkProcessor2;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.processor.statistics.ClassifiedFlowsStatisticsProcessor;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePostPlaceScriptCluster2;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;
import java.util.logging.Level;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/25/11
 * Time: 9:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestPlacement2 {


    public static void main(String[] args) {
        Util.logger.setLevel(Level.ALL);
        boolean checkLinks = false;

        String topologyFile = "input/osdi/topology/100/tree_1000_100.txt";
        String flowFile = "input/osdi/flows/100/1/classbench_32768_1.txt";
        String partitionFile = "input/osdi/partition/equal2/1024_classbench_32768_1.txt";
        String statOutputFile = "output/testplacement.txt";
        String statOutputFile2 = "output/testplacement2.txt";
        String assignmentOutputFile = "output/testassignment.txt";
        long start = System.currentTimeMillis();
        try {
            Map<String, Object> parameters = new HashMap<String, Object>();
            List<Partition> partitions = Util.loadFile(new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()), partitionFile, parameters, new LinkedList<Partition>());

            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()),
                    topologyFile, parameters, new LinkedList<Topology>()).get(0);

            Collection<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology), flowFile, parameters, new LinkedList<Flow>());
            long time0 = System.currentTimeMillis();
            System.out.println("load " + (time0 - start) / 1000.0);
            PartitionClassifier pc = new TwoLevelTrafficProcessor(new LinearMatchTrafficProcessor(), new LinearMatchTrafficProcessor());
            final Map<Partition, Map<Rule, Collection<Flow>>> classified = pc.classify(flows, partitions);
            long time1 = System.currentTimeMillis();
            System.out.println("classify " + (time1 - time0) / 1000.0);
            time0 = time1;
            MinTrafficSwitchSelection switchSelection = new MinTrafficSwitchSelection();
            MaxSizePartitionSorter partitionSelection = new MaxSizePartitionSorter();
            Map<Switch, Collection<Partition>> sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(classified);
            Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, topology, sourcePartitions);
            Placer placer = new Assigner(switchSelection, partitionSelection, 100, checkLinks, forwardingRules, sourcePartitions);
            parameters.put("placement.alg", placer);
            parameters.put("placement." + placer + ".switchSelection", switchSelection);
            parameters.put("placement." + placer + ".partitionSelection", partitionSelection);
            final Assignment assignment = placer.place(topology, partitions);
            time1 = System.currentTimeMillis();
            System.out.println("place " + (time1 - time0) / 1000);
            time0 = time1;
            //
            RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
            System.out.println(Statistics.getParameterLine(parameters));
            runFlowsOnNetworkProcessor2.print(new PrintWriter(System.out));

            runFlowsOnNetworkProcessor2.process(topology, flows);
            time1 = System.currentTimeMillis();
            System.out.println("route " + (time1 - time0) / 1000);
            time0 = time1;
            //
            Statistics stats = topology.getStat(parameters);
            stats.joinStats(new ClassifiedFlowsStatisticsProcessor(classified, parameters, null).run());
            final ArrayList<String> parameterNames = new ArrayList<String>(parameters.keySet());
            Util.writeFile(Statistics.csvStatistics(parameterNames, Statistics.categorize(parameterNames, Arrays.asList(stats)),
                    stats.getStatNames(), true, true), new File(statOutputFile), true);
            //final Map<List<Statistics.Parameter>, List<Statistics>> categorizedStats = Statistics.categorize(Arrays.asList("FlowFile"), Arrays.asList(stats));
            /*new SaveFileProcessor<String>(stats.getParameterLine() + "\n" +
                    Statistics.getMeanLine(categorizedStats, PlacementStatisticsProcessor.TRAFFIC_STAT), new File(statOutputFile), true).run();*/
            time1 = System.currentTimeMillis();
            System.out.println("stats " + (time1 - time0) / 1000);
            WriterSerializableUtil.writeFile(assignment, new File(assignmentOutputFile), false, parameters);

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
