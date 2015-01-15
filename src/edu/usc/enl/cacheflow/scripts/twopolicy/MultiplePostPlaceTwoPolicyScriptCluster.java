package edu.usc.enl.cacheflow.scripts.twopolicy;

import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.algorithms.PostPlacer;
import edu.usc.enl.cacheflow.algorithms.migration.DeterministicMigrateVMStartPartitions;
import edu.usc.enl.cacheflow.algorithms.migration.rmigration3.RMigrateVMStartPartition3;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.ThreadMinTrafficSwitchSelection;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.TrafficAwareSwitchSelection;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.network.VMStartMigrateProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePostPlaceScriptCluster2;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.util.*;
import java.util.logging.Level;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/1/12
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class MultiplePostPlaceTwoPolicyScriptCluster {
    private static Map<Partition, Map<Rule, Collection<Flow>>> backupRuleFlowMap;

//    public static Map<Long, Partition> debugPartitionMap;

    public static void main(String[] args) throws Exception {
        Util.threadNum = Integer.parseInt(args[0]);
        int randomSeedIndex = Integer.parseInt(args[1]);
        String maxTopology = args[2];
        File topologyFolder = new File(args[3]);
        File flowFile = new File(args[4]);
        File partitionFile = new File(args[5]);
        String outputFolder = args[6];
        File clusterFolder = new File(args[7]);


        Map<String, Object> parameters = new HashMap<String, Object>();
        //run(flowsFile, partitionFile, topologyFolder, parameters, maxTopology, randomSeedIndex, outputFolder);
        //laundry stuff
        Util.setRandom(randomSeedIndex);
        Util.logger.setLevel(Level.WARNING);
        {
            File outputFolderFile = new File(outputFolder);
            outputFolderFile.mkdirs();
        }


        //load data
        final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
        //final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>());
        List<Partition> partitions = Util.loadFileFilterParam(partitionFactory, partitionFile.getPath(), parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");
        String maxTopologyPath = topologyFolder.getPath() + "/" + maxTopology;
        Topology simTopology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), maxTopologyPath, parameters, new ArrayList<Topology>(1)).get(0);
        List<Flow> flows = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                simTopology), flowFile.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");

        final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows = new ThreadTwoLevelTrafficProcessor(
                new OVSClassifier(),
                new OVSClassifier(), Util.threadNum).classify(flows, partitions);


        //prepare minimum stats
        Map<Partition, Long> minTraffic = MultiplePostPlaceScriptCluster2.getMinOverhead(simTopology, classifiedFlows);

        Map<Switch, Collection<Partition>> sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(classifiedFlows);
        Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, simTopology, sourcePartitions);

        simTopology.createHelpers(partitions, forwardingRules, classifiedFlows);
        simTopology.setRuleFlowMap(classifiedFlows);

//        backupRuleFlowMap = new HashMap<>(classifiedFlows.size());
//        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry : classifiedFlows.entrySet()) {
//            Map<Rule, Collection<Flow>> newEntry = new HashMap<>(entry.getValue().size());
//            for (Map.Entry<Rule, Collection<Flow>> entry2 : entry.getValue().entrySet()) {
//                Collection<Flow> newFlows = new ArrayList<>(entry2.getValue());
//                for (Flow flow : entry2.getValue()) {
//                    newFlows.add(flow.duplicate());
//                }
//                newEntry.put(entry2.getKey(), newFlows);
//            }
//            backupRuleFlowMap.put(entry.getKey(), newEntry);
//        }
//
        UnifiedPartitionFactory partitionFactoryBase = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
        Util.loadFileFilterParam(partitionFactoryBase, "input\\nsdi\\classbenchpartition\\vmstart\\imcsplit\\1\\20480_classbench_131072_2.txt", parameters, new LinkedList<Partition>(),"(partition|rule)\\..*");
        MultiplePlacementTwoPolicyScript.resetToMatrixRuleSet(partitionFactory, partitions, simTopology, forwardingRules, partitionFactoryBase);


        //create objects
//        ClusterSelection partitionSelection = new MaxRuleSizeClusterSelection();
//        SwitchSelectionCluster switchSelection = new RandomSwitchSelectionCluster(Util.random);
//        Placer placer = new AssignerCluster(switchSelection, partitionSelection, 100, false, forwardingRules, null, sourcePartitions);
//        parameters.put("placement." + placer + ".partitionSelection", partitionSelection);
//        parameters.put("placement." + placer + ".switchSelection", switchSelection);

        Placer placer = new AssignmentFactory.LoadPlacer(false, forwardingRules,
                "input\\nsdi\\classbenchassignmenttwopolicy\\imc\\original\\-3\\2\\memory\\2\\assignment",
//                "output\\nsdi\\classbench\\imc\\fmigrate\\original\\-1\\cpu\\2_det1\\3_postplacement",
                parameters, partitions, sourcePartitions);
        parameters.put("placement.alg", placer);

        ////////////////////////////////////////////////////////////////////////////////////

        final VMStartMigrateProcessor vmMigrator = new VMStartMigrateProcessor(simTopology, Util.random, Util.threadNum, classifiedFlows,
                minTraffic, sourcePartitions, forwardingRules, 1);
        parameters.put("postPlacement0.alg", vmMigrator);

        ////////////////////////////////////////////////////////////////////////////////////

//        ThreadMinTrafficSwitchReplicateSelectionCluster postPlacerSwitchSelection = new ThreadMinTrafficSwitchReplicateSelectionCluster(Util.threadNum);
//        PostPlacer postPlacer = new ReplicateCluster(placer.getForwardingRules(),
//                replicateSwitchSelection, simTopology, false, classifiedFlows, clusters, minTraffic, partitionSources);

//        TrafficAwareSwitchSelection postPlacerSwitchSelection = new ThreadMinTrafficSameRuleSwitchSelection(Util.threadNum);
//        PostPlacer postPlacer = new DeterministicMigrateVMStartPartitions(
//                postPlacerSwitchSelection, simTopology, false, classifiedFlows, minTraffic,  sourcePartitions, forwardingRules);


//        debugPartitionMap=new HashMap<Long, Partition>();
//        for (Partition partition : partitions) {
//            debugPartitionMap.put(partition.getProperty(Util.getDimensionInfoIndex(Util.SRC_IP_INFO)).getStart(),partition);
//        }

//        final int betaSwitchSelection = 5;
//        final int betaPartitionSelection = 0;
//        final double chanceRatio = 1.01;
//        final RandomMinTrafficSwitchSelection postPlacerSwitchSelection = new RandomMinTrafficSwitchSelection(Util.random,
//                Util.threadNum, betaSwitchSelection, classifiedFlows);
//        PostPlacer postPlacer = new RMigrateVMStartPartition2(postPlacerSwitchSelection,
//                simTopology, minTraffic, sourcePartitions, forwardingRules, Util.random,
//                100, 1, betaPartitionSelection, chanceRatio, true);
//        parameters.put("postPlacement1." + postPlacer + "." + postPlacerSwitchSelection + ".beta", betaSwitchSelection);
//        parameters.put("postPlacement1." + postPlacer + ".beta", betaPartitionSelection);
//        parameters.put("postPlacement1." + postPlacer + ".chanceRatio", chanceRatio);
//
//
//        parameters.put("postPlacement1.alg", postPlacer);
//        parameters.put("postPlacement1." + postPlacer + ".switchSelection", postPlacerSwitchSelection);

        ////////////////////////////////////////////////////////////////////////////////////

        final int initDownSteps = 1;
        final double alphaDownSteps = 0.5;
        PostPlacer postPlacer = new RMigrateVMStartPartition3(
                simTopology, minTraffic, sourcePartitions, forwardingRules, Util.threadNum,
                100, 1, initDownSteps, alphaDownSteps, 10000);
        parameters.put("postPlacement1." + postPlacer + ".initDownSteps", initDownSteps);
        parameters.put("postPlacement1." + postPlacer + ".alphaDownSteps", alphaDownSteps);

        parameters.put("postPlacement1.alg", postPlacer);

        ////////////////////////////////////////////////////////////////////////////////////

        TrafficAwareSwitchSelection postPlacerSwitchSelection2 = new ThreadMinTrafficSwitchSelection(Util.threadNum);
        PostPlacer postPlacer2 = new DeterministicMigrateVMStartPartitions(
                postPlacerSwitchSelection2, simTopology, minTraffic, sourcePartitions, forwardingRules);


        parameters.put("postPlacement2.alg", postPlacer2);
        parameters.put("postPlacement2." + postPlacer2 + ".switchSelection", postPlacerSwitchSelection2);


        ////////////////////////////////////////////////////////////////////////////////////

        MultiplePostPlaceScriptCluster2.runForTopologiesN(maxTopology, topologyFolder, outputFolder, parameters, simTopology, flows, placer, clusterFolder,
                sourcePartitions, partitions, forwardingRules, Arrays.asList(postPlacer2,postPlacer2,postPlacer2));

    }


}
