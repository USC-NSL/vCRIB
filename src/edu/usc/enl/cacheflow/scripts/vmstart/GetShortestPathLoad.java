package edu.usc.enl.cacheflow.scripts.vmstart;

import edu.usc.enl.cacheflow.algorithms.placement.VMStartAssigner;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.network.RunFlowsOnNetworkProcessor2;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePostPlaceScriptCluster2;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/26/11
 * Time: 11:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetShortestPathLoad {
    public static void main(String[] args) {

        Util.threadNum = Integer.parseInt(args[0]);
        String partitionFile = args[1];
        String topologyFile = args[2];
        String flowFile = args[3];
        String vmsFile = args[4];
        String statOutputFile = args[5];
        String runFlowStats = args[6];
        boolean append = Boolean.parseBoolean(args[7]);

        new File(statOutputFile).getParentFile().mkdirs();
        new File(runFlowStats).getParentFile().mkdirs();
        place(partitionFile, topologyFile, flowFile, new File(vmsFile), statOutputFile, runFlowStats, append, true);
    }

    /*public static void place(String ruleFile, String topologyFile, String flowFile, String statOutputFile, String runFlowStatsFile, boolean append) {
        try {
            final Map<String, Object> parameters = new HashMap<String, Object>();
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                    Util.createDefaultAggregator(), new HashSet<Rule>()), topologyFile, parameters, new ArrayList<Topology>(1)).get(0);
            List<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology), flowFile,
                    parameters, new LinkedList<Flow>());

            //parameters.put("alg", "Minimal");
            //setMinimalRules(ruleFile,topology, flows);


            //new MaximalRuleLoadProcessor().process(topology, rules);
            parameters.put("placement.alg", "VMStart");
            final Collection<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()), ruleFile,
                    parameters, new HashSet<Rule>());
            new VMStartRuleLoadProcessor().process(topology, rules, flows);

//            final OVSClassifier ovsClassifier = new OVSClassifier();
//            new MinimalRuleLoadProcessor().process(topology, ovsClassifier.classify(flows, rules));

            RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
            PrintWriter runFlowStatsWriter = new PrintWriter(new BufferedWriter(new FileWriter(runFlowStatsFile, append)));
            runFlowStatsWriter.println(Statistics.getParameterLine(parameters));
            runFlowsOnNetworkProcessor2.process(topology, flows);
            runFlowsOnNetworkProcessor2.print(new PrintWriter(runFlowStatsWriter));
            runFlowStatsWriter.println();
            runFlowStatsWriter.close();

            Statistics stats = topology.getStat(parameters);

            Util.writeFile(Statistics.csvStatistics(parameters.keySet(), Statistics.categorize(parameters.keySet(), Arrays.asList(stats)),
                    stats.getStatNames(), true, !append), new File(statOutputFile), append);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/


    public static void place(String partitionFile, String topologyFile, String flowFile, File vmsFile, String statOutputFile,
                             String runFlowStatsFile, boolean append, boolean srcOrToR) {
        try {
            Rule.resetMaxId();
            Util.resetDimensionInfos();
            final Map<String, Object> parameters = new HashMap<String, Object>();
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                    Util.createDefaultAggregator(), new HashSet<Rule>()), topologyFile, parameters, new ArrayList<Topology>(1)).get(0);
            List<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology), flowFile,
                    parameters, new LinkedList<Flow>());

            //load data
//            final BufferedReader br = new BufferedReader(new FileReader(partitionFile));
//            final LinkedList<Rule> rules = new LinkedList<Rule>();
//            new RuleFactory(new FileFactory.EmptyLineStopCondition()).create(br, parameters, rules);
//            //add zombie rules for forwarding rules
//            List<RangeDimensionRange> allRangeProperties = new ArrayList<RangeDimensionRange>();
//            for (DimensionInfo dimensionInfo : Util.getDimensionInfos()) {
//                allRangeProperties.add(dimensionInfo.getDimensionRange());
//            }
//            for (int i = 0; i < 20480; i++) {//load this number from properties partition.num
//                rules.add(new Rule(DenyAction.getInstance(), allRangeProperties, -1, Rule.maxId + 1));
//            }
//
//            MatrixRuleSet.setRuleSet(rules);
//            final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new MatrixRuleSet(), rules);
//            //final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>());
//            final LinkedList<Partition> partitions = new LinkedList<Partition>();
//            partitionFactory.create(br, parameters, partitions);
//            br.close();
//
//            for (Switch aSwitch : topology.getSwitches()) {
//                final MatrixRuleSet partitionRules = new MatrixRuleSet();
//                aSwitch.setState(aSwitch.isFeasible(partitionRules, 0, false, false));
//            }

            final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
            List<Partition> partitions = Util.loadFileFilterParam(partitionFactory, partitionFile, parameters, new LinkedList<Partition>(), "partition\\..*");


            parameters.put("placement.alg", "VMStart");
            final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows = new ThreadTwoLevelTrafficProcessor(
                    new OVSClassifier(),
                    new OVSClassifier(), Util.threadNum).classify(flows, partitions);
            Map<Long, Switch> vmSource = null;
            Map<Switch, Collection<Partition>> sourcePartitions;
            try {
                if (vmsFile != null) {
                    vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology), vmsFile.getPath(),
                            parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
                    sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(partitions, vmSource);
                } else {
                    sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(classifiedFlows);
                }
            } catch (IOException e) {
                sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(classifiedFlows);
            }

            //classifiedFlows.clear();
            Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, topology, sourcePartitions);
            //partitions.clear();


            topology.createHelpers(partitions, forwardingRules, classifiedFlows);
            topology.setRuleFlowMap(classifiedFlows);

            MultiplePlacementScript.resetToMatrixRuleSet(partitionFactory, partitions, topology, forwardingRules);
//            ListIterator<Rule> rulesListIterator = rules.listIterator(rules.size() - 20480);
//            for (Map<Switch, Rule> entry : forwardingRules.values()) {
//                for (Rule rule : entry.values()) {
//                    final int id = rulesListIterator.next().getId();
//                    rule.setId(id);
//                    rulesListIterator.set(rule);
//                }
//            }

            new VMStartAssigner(forwardingRules, sourcePartitions, topology, srcOrToR).postPlace(null, null, null);
            //forwardingRules.clear();

            RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
            PrintWriter runFlowStatsWriter = new PrintWriter(new BufferedWriter(new FileWriter(runFlowStatsFile, append)));
            runFlowStatsWriter.println(Statistics.getParameterLine(parameters));
            runFlowsOnNetworkProcessor2.process(topology, flows);
            runFlowsOnNetworkProcessor2.print(new PrintWriter(runFlowStatsWriter));
            runFlowStatsWriter.println();
            runFlowStatsWriter.close();

            Statistics stats = topology.getStat(parameters);

            Util.writeFile(Statistics.csvStatistics(parameters.keySet(), Statistics.categorize(parameters.keySet(), Arrays.asList(stats)),
                    stats.getStatNames(), true, !append), new File(statOutputFile), append);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
