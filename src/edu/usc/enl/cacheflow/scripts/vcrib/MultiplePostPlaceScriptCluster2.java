package edu.usc.enl.cacheflow.scripts.vcrib;

import edu.usc.enl.cacheflow.algorithms.migration.rmigration3.RMigrateVMStartPartition3;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.ThreadMinTrafficSwitchSelection;
import edu.usc.enl.cacheflow.algorithms.migration.*;
import edu.usc.enl.cacheflow.algorithms.placement.AssignerCluster;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.TrafficAwareSwitchSelection;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.file.SaveFileProcessor;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.network.InformOnRestart;
import edu.usc.enl.cacheflow.processor.network.RunFlowsOnNetworkProcessor2;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.algorithms.PostPlacer;
import edu.usc.enl.cacheflow.processor.network.VMStartMigrateRandomProcessor;
import edu.usc.enl.cacheflow.processor.partition.SourcePartitionFinder;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
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
public class MultiplePostPlaceScriptCluster2 {
    private static Map<Partition, Map<Rule, Collection<Flow>>> backupRuleFlowMap;

//    public static Map<Long, Partition> debugPartitionMap;

    public static void main(String[] args) throws Exception {
        Util.threadNum = Integer.parseInt(args[0]);
        int randomSeedIndex = Integer.parseInt(args[1]);
        String maxTopology = args[2];
        File topologyFolder = new File(args[3]);
        File flowFile = new File(args[4]);
        File vmFile = new File(args[5]);
        File partitionFile = new File(args[6]);
        String outputFolder = args[7];
        String assignmentFolder=args[8];
        File clusterFolder = new File(args[9]);


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
        Map<Partition, Long> minTraffic = getMinOverhead(simTopology, classifiedFlows);

        Map<Long, Switch> vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), simTopology), vmFile.getPath(),
                parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
        final Map<Switch, Collection<Partition>> sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(partitions, vmSource);
        //Map<Switch, Collection<Partition>> sourcePartitions = getSourcePartitions(classifiedFlows);
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

        MultiplePlacementScript.resetToMatrixRuleSet(partitionFactory, partitions, simTopology, forwardingRules);


        //create objects
//        ClusterSelection partitionSelection = new MaxRuleSizeClusterSelection();
//        SwitchSelectionCluster switchSelection = new RandomSwitchSelectionCluster(Util.random);
//        Placer placer = new AssignerCluster(switchSelection, partitionSelection, 100, false, forwardingRules, null, sourcePartitions);
//        parameters.put("placement." + placer + ".partitionSelection", partitionSelection);
//        parameters.put("placement." + placer + ".switchSelection", switchSelection);

        Placer placer = new AssignmentFactory.LoadPlacer(false, forwardingRules,assignmentFolder,
//                "input\\nsdi\\classbenchassignment\\imcdiffvmfixed\\original\\-3\\2\\memory\\assignment",

                //"output\\nsdi\\classbench\\imcdiffvmfixed\\fmigrate\\original\\-3\\memory\\2_det1\\2_postplacement",
                //"output\\nsdi\\classbench\\imcdiffvmfixed\\fmigrate\\original6\\-3\\memory\\2_greedynewalphat\\0_postplacement",
                //"output\\nsdi\\classbench\\imcdiffvmfixed\\fmigrate\\original6\\-3\\memory\\2_greedymore\\2_postplacement",
                parameters, partitions, sourcePartitions);
        parameters.put("placement.alg", placer);

        ////////////////////////////////////////////////////////////////////////////////////

        final PostPlacer vmMigrator = new VMStartMigrateRandomProcessor(simTopology, Util.random, Util.threadNum, classifiedFlows,
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

//        final int betaSwitchSelection = 10;
////        final int betaPartitionSelection = 0;
//        final double chanceRatio = 0;
//        final RandomMinTrafficSwitchSelection postPlacerSwitchSelection = new RandomMinTrafficSwitchSelection(Util.random,
//                Util.threadNum, betaSwitchSelection, classifiedFlows);
////        PostPlacer postPlacer = new RealRandomMigrateVMStartPartitions(
////                simTopology,minTraffic,sourcePartitions,forwardingRules,Util.random,Util.threadNum,100,1,betaSwitchSelection,
////                1,1
////        );
//        PostPlacer postPlacer = new RMigrateVMStartPartition2(postPlacerSwitchSelection, simTopology, minTraffic, sourcePartitions, forwardingRules, Util.random,
//                100, 1, 0, chanceRatio, false);
//        parameters.put("postPlacement1." + postPlacer + "." + postPlacerSwitchSelection + ".beta", betaSwitchSelection);
//        //parameters.put("postPlacement1." + postPlacer + ".beta", betaPartitionSelection);
//        parameters.put("postPlacement1." + postPlacer + ".chanceRatio", chanceRatio);
//
//
//        parameters.put("postPlacement1.alg", postPlacer);
//        parameters.put("postPlacement1." + postPlacer + ".switchSelection", postPlacerSwitchSelection);

        ////////////////////////////////////////////////////////////////////////////////////

        final int initDownSteps = 1;
        final double alphaDownSteps = 0.5;
        int timeBudget = 0;
        PostPlacer postPlacer = new RMigrateVMStartPartition3(
                simTopology, minTraffic, sourcePartitions, forwardingRules, Util.threadNum,
                100, 1, initDownSteps, alphaDownSteps, timeBudget);
        parameters.put("postPlacement1." + postPlacer + ".initDownSteps", initDownSteps);
        parameters.put("postPlacement1." + postPlacer + ".alphaDownSteps", alphaDownSteps);
        parameters.put("postPlacement1." + postPlacer + ".timeBudget", timeBudget);

        parameters.put("postPlacement1.alg", postPlacer);

        ////////////////////////////////////////////////////////////////////////////////////

        TrafficAwareSwitchSelection postPlacerSwitchSelection2 = new ThreadMinTrafficSwitchSelection(Util.threadNum);
        PostPlacer postPlacer2 = new DeterministicMigrateVMStartPartitions(
                postPlacerSwitchSelection2, simTopology, minTraffic, sourcePartitions, forwardingRules);


        parameters.put("postPlacement2.alg", postPlacer2);
        parameters.put("postPlacement2." + postPlacer2 + ".switchSelection", postPlacerSwitchSelection2);


        ////////////////////////////////////////////////////////////////////////////////////
        List<PostPlacer> postPlacers = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            postPlacers.add(vmMigrator);
            postPlacers.add(postPlacer);
        }


        runForTopologiesN(maxTopology, topologyFolder, outputFolder, parameters, simTopology, flows, placer, clusterFolder,
                sourcePartitions, partitions, forwardingRules, Arrays.asList(postPlacer));

    }

    public static Map<Switch, Collection<Partition>> getSourcePartitions(Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows) {
        Map<Switch, Collection<Partition>> output = new HashMap<Switch, Collection<Partition>>();
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry : classifiedFlows.entrySet()) {
            for (Collection<Flow> flows : entry.getValue().values()) {
                if (flows.size() > 0) {
                    Flow flow = flows.iterator().next();
                    Switch source = flow.getSource();
                    Collection<Partition> partitions = output.get(source);
                    if (partitions == null) {
                        partitions = new HashSet<Partition>();
                        output.put(source, partitions);
                    }
                    partitions.add(entry.getKey());
                    //break; //break because VMSTART PARTITIONS
                }
            }
        }
        return output;
    }

    public static Map<Switch, Collection<Partition>> getSourcePartitions(List<Partition> partitions, Map<Long, Switch> switchSourceIps) {
        return new SourcePartitionFinder().process(partitions, switchSourceIps);
    }

    public static void runForTopologiesN(String maxTopology, File topologyFolder, String outputFolder, Map<String, Object> parameters,
                                         Topology simTopology, List<Flow> flows,
                                         Placer placer, File clusterFolder,
                                         Map<Switch, Collection<Partition>> sourcePartitions,
                                         List<Partition> partitions, Map<Partition, Map<Switch, Rule>> forwardingRules,
                                         List<PostPlacer> postplacers) throws Exception {
        boolean successful = false;
        int runNumber = 1;
        {
            System.out.println(runNumber + ": " + maxTopology);
            new File(outputFolder + "/placement").mkdirs();
            if (placer instanceof AssignerCluster) {
                List<Cluster> clusters = AssignerCluster.loadClusters(parameters, clusterFolder, partitions);
                ((AssignerCluster) placer).setClusters(clusters);
            }
            successful = runForTopologyN(simTopology, parameters, true, flows, outputFolder, placer,
                    outputFolder + "/placement",
                    sourcePartitions, forwardingRules, postplacers, maxTopology);
        }
        runNumber++;
        if (successful) {
            final List<File> topologyFiles = new ArrayList<File>(Arrays.asList(topologyFolder.listFiles(Util.TXT_FILTER)));
            Collections.sort(topologyFiles);
            for (File topologyFile : topologyFiles) {
                final String topologyFileName = topologyFile.getName();
                if (topologyFileName.equals(maxTopology)) {
                    continue;
                }
                System.out.println(runNumber + ": " + topologyFileName);

                //update topology
                {
                    Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                            new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), topologyFile.getPath(),
                            parameters, new ArrayList<Topology>(1)).get(0);
                    for (Switch aSwitch : topology.getSwitches()) {
                        simTopology.getSwitchMap().get(aSwitch.getId()).fillParam(aSwitch);
                    }
                }
                if (placer instanceof AssignerCluster) {
                    List<Cluster> clusters = AssignerCluster.loadClusters(parameters, clusterFolder, partitions);
                    ((AssignerCluster) placer).setClusters(clusters);
                }

                runForTopologyN(simTopology, parameters, false, flows, outputFolder, placer,
                        outputFolder + "/placement",
                        sourcePartitions, forwardingRules, postplacers, topologyFileName);
                runNumber++;
            }
        }
        System.out.println("finished");
    }

    public static boolean runForTopologyN(Topology topology,
                                           Map<String, Object> parameters, boolean firstRun, List<Flow> flows,
                                           String outputFolder, Placer placer,
                                           String placementOutputFolder,
                                           Map<Switch, Collection<Partition>> sourcePartitions,
                                           Map<Partition, Map<Switch, Rule>> forwardingRules,
                                           List<PostPlacer> postPlacers,
                                           String topologyName) throws Exception {
        boolean successful = false;

        try {
            Assignment assignment = placer.place(topology, forwardingRules.keySet());

//            System.out.println("Run flow");
//            RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
//            runFlowsOnNetworkProcessor2.process(topology, flows);

            WriterSerializableUtil.writeFile(assignment, new File(placementOutputFolder + "/assignment.txt"), false, parameters);
            successful = true;
            //now do postplacement
            Map<Partition, Map<Switch, Switch>> postPlaceResult = null;
            for (int i = 0; i < postPlacers.size(); i++) {
                String postPlacementFolder = outputFolder + "/" + i + "_postplacement";
                new File(postPlacementFolder).mkdirs();
                PrintWriter trendWriter = new PrintWriter(new BufferedWriter(new FileWriter(postPlacementFolder + "/" + "trend" + topologyName)));
                if (i == 0) {
                    postPlaceResult = doPostAssignment(topology, parameters, firstRun,
                            flows, postPlacementFolder, placer.getLastAvailableSwitches(), postPlacers.get(i),
                            trendWriter, assignment, topologyName);
                } else {
                    Map<Partition, Switch> placement = new HashMap<Partition, Switch>(postPlaceResult.size());
                    for (Map.Entry<Partition, Map<Switch, Switch>> entry : postPlaceResult.entrySet()) {
                        placement.put(entry.getKey(), entry.getValue().values().iterator().next());
                    }
                    postPlaceResult = doPostAssignment(topology, parameters, firstRun, flows, postPlacementFolder,
                            topology.getSwitches(), postPlacers.get(i), trendWriter, new Assignment(placement), topologyName);
                }
                trendWriter.close();
                writePostPlaceResult(postPlaceResult, new File(postPlacementFolder + "/" + "assignment_" + topologyName), parameters);


            }
            for (PostPlacer postPlacer : postPlacers) {
                if (postPlacer instanceof InformOnRestart) {
                    ((InformOnRestart) postPlacer).restart();

                }
            }

        } catch (NoAssignmentFoundException e) {
            e.printStackTrace();
            Util.logger.warning("No assignment found for " + Statistics.getParameterLine(parameters));
        }

        MultiplePlacementScript.writeSolutionCSV(parameters, firstRun, outputFolder, successful);
        topology.reset();

        return successful;
    }


    public static void writePostPlaceResult(Map<Partition, Map<Switch, Switch>> postPlaceResult, File outputFile, Map<String, Object> parameters) throws IOException {
        Map<Switch, Collection<Partition>> rPlacement = new HashMap<>();
        for (Map.Entry<Partition, Map<Switch, Switch>> entry1 : postPlaceResult.entrySet()) {
            for (Switch host : entry1.getValue().values()) {
                Collection<Partition> partitions = rPlacement.get(host);
                if (partitions == null) {
                    partitions = new LinkedList<>();
                    rPlacement.put(host, partitions);
                }
                partitions.add(entry1.getKey());
            }
        }

        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
        writer.println(Statistics.getParameterLine(parameters));
        for (Map.Entry<Switch, Collection<Partition>> entry : rPlacement.entrySet()) {
            writer.print(entry.getKey().getId());
            for (Partition partition : entry.getValue()) {
                writer.print("," + partition.getId());
            }
            writer.println();
        }
        writer.close();
    }

    private static Map<Partition, Map<Switch, Switch>> doPostAssignment(Topology topology, Map<String, Object> parameters, boolean firstRun,
                                                                        List<Flow> flows, String outputFolder, Collection<Switch> lastAvailableSwitches,
                                                                        PostPlacer postPlacer, PrintWriter postPlacementTrendWriter, Assignment assignment,
                                                                        String topologyName
    ) throws Exception {


        postPlacementTrendWriter.println(Statistics.getParameterLine(parameters));
        Map<Partition, Map<Switch, Switch>> partitionSourceReplica =
                postPlacer.postPlace(new HashSet<Switch>(lastAvailableSwitches), assignment, postPlacementTrendWriter);
        postPlacementTrendWriter.flush();

        postPlacementTrendWriter.println();
        postPlacementTrendWriter.flush();
        //new SetReplicateForwardingRulesProcessor().process(partitionSourceReplica, topology);
        System.out.println("Run flow");
        RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
        runFlowsOnNetworkProcessor2.process(topology, flows);
        PrintWriter runFlowStatsWriter = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder + "/postplacement_runflows_" + topologyName, false)), true);
        runFlowStatsWriter.println(Statistics.getParameterLine(parameters));


        runFlowsOnNetworkProcessor2.print(runFlowStatsWriter);
        runFlowStatsWriter.println();
        runFlowStatsWriter.close();

        Statistics postPlaceStats = topology.getStat(parameters);
        postPlaceStats.joinStats(postPlacer.getStats(parameters));

        new SaveFileProcessor<String>(Statistics.csvStatistics(parameters.keySet(), Statistics.categorize(parameters.keySet(),
                Collections.singleton(postPlaceStats)), postPlaceStats.getStatNames(), true, firstRun),
                new File(outputFolder + "/postplacement.csv"), !firstRun).run();
        return partitionSourceReplica;
    }

    public static Map<Partition, Long> getMinOverhead(Topology topology, Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows) {
        Map<Partition, Long> output = new HashMap<Partition, Long>();
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry0 : classifiedFlows.entrySet()) {
            long sum = 0;
            for (Map.Entry<Rule, Collection<Flow>> entry : entry0.getValue().entrySet()) {
                for (Flow flow : entry.getValue()) {
                    if (entry.getKey().getAction().doAction(flow) != null) {
                        sum += topology.getPathLength(flow.getSource(), flow.getDestination()) * flow.getTraffic();
                    }
                }
            }
            output.put(entry0.getKey(), sum);
        }
        return output;
    }
}
