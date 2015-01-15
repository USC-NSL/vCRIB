package edu.usc.enl.cacheflow.scripts.vcrib;

import edu.usc.enl.cacheflow.algorithms.feasibility.general.FeasiblePlacer2;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.ForwardAction;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.file.SaveFileProcessor;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.network.RunFlowsOnNetworkProcessor2;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.logging.Level;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/1/12
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class MultiplePlacementScript {

    public static final String SOLUTION_METRIC = "solution";

    public static void main(String[] args) throws Exception {
        int randomSeedIndex = Integer.parseInt(args[0]);
        Util.threadNum = Integer.parseInt(args[1]);
        String maxTopology = args[2];
        File topologyFolder = new File(args[3]);
        File flowFile = new File(args[4]);
        File vmFile = new File(args[5]);
        File partitionFile = new File(args[6]);
        String outputFolder = args[7];

        Map<String, Object> parameters = new HashMap<String, Object>();
        //run(flowsFile, partitionFile, topologyFolder, parameters, maxTopology, randomSeedIndex, outputFolder);
        //laundry stuff
        Util.setRandom(randomSeedIndex);
        Util.logger.setLevel(Level.WARNING);
        new File(outputFolder).mkdirs();

        //load data
        long start = System.currentTimeMillis();

        final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
        List<Partition> partitions = Util.loadFile(partitionFactory, partitionFile.getPath(), parameters, new LinkedList<Partition>());

        System.out.println("load partition " + (System.currentTimeMillis() - start));

        String maxTopologyPath = topologyFolder.getPath() + "/" + maxTopology;
        Topology simTopology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), maxTopologyPath, parameters, new ArrayList<Topology>()).get(0);
        List<Flow> flows = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                simTopology), flowFile.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");

        System.out.println("load flows " + (System.currentTimeMillis() - start));

        final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows = new ThreadTwoLevelTrafficProcessor(
                new OVSClassifier(),
                new OVSClassifier(), Util.threadNum).classify(flows, partitions);

        System.out.println("classfiy " + (System.currentTimeMillis() - start));

        Map<Long, Switch> vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), simTopology), vmFile.getPath(),
                parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
        final Map<Switch, Collection<Partition>> sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(partitions, vmSource);
        //final Map<Switch, Collection<Partition>> sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(classifiedFlows);
        Map<Partition, Map<Switch, Rule>> forwardingRules = createForwardingRules(partitions, simTopology, sourcePartitions);

        System.out.println("forwarding rules " + (System.currentTimeMillis() - start));

        simTopology.createHelpers(partitions, forwardingRules, classifiedFlows);
        simTopology.setRuleFlowMap(classifiedFlows);

        System.out.println("helpers " + (System.currentTimeMillis() - start));

        MultiplePlacementScript.resetToMatrixRuleSet(partitionFactory, partitions, simTopology, forwardingRules);

        System.out.println("reset rulesets " + (System.currentTimeMillis() - start));

        Placer placer = new FeasiblePlacer2(false, forwardingRules, Util.threadNum, sourcePartitions);


        //Placer placer = new Assigner(switchSelection, partitionSorter, 100, false, forwardingRules);
//        parameters.put("placement." + placer + ".partitionSelection", partitionSorter);
//        parameters.put("placement." + placer + ".switchSelection", switchSelection);

        parameters.put("placement.alg", placer);

        //run for max configuration
        runForTopologies(maxTopology, topologyFolder, outputFolder, parameters, simTopology, flows, placer, partitions);

    }

    public static Map<Partition, Map<Switch, Rule>> createForwardingRules(Collection<Partition> partitions, Topology simTopology,
                                                                          Map<Switch, Collection<Partition>> sourcesPartition) {
        Map<Partition, Map<Switch, Rule>> forwardingRules = new HashMap<Partition, Map<Switch, Rule>>(sourcesPartition.size());
        for (Partition partition : partitions) {
            forwardingRules.put(partition, new HashMap<Switch, Rule>());
        }
        for (Map.Entry<Switch, Collection<Partition>> entry : sourcesPartition.entrySet()) {
            for (Partition partition : entry.getValue()) {
                forwardingRules.get(partition).put(entry.getKey(),
                        new Rule(new ForwardAction(simTopology.getControllerSwitch()),
                                partition.getProperties(), -1, Rule.maxId + 1));
            }
        }
        return forwardingRules;
    }

    public static void runForTopologies(String maxTopology, File topologyFolder, String outputFolder, Map<String, Object> parameters,
                                        Topology simTopology, List<Flow> flows,
                                        Placer placer, List<Partition> partitions) throws Exception {
        boolean successful;
        int runNumber = 1;
        {
            System.out.println("run number " + runNumber);
            new File(outputFolder + "/assignment").mkdirs();
            successful = runForTopology(simTopology, parameters, true, flows, outputFolder, placer,
                    new File(outputFolder + "/assignment/assignment_" + maxTopology), partitions);
            runNumber++;
        }
        if (successful) {
            final List<File> topologyFiles = new ArrayList<File>(Arrays.asList(topologyFolder.listFiles(Util.TXT_FILTER)));
            Collections.sort(topologyFiles);
            for (File topologyFile : topologyFiles) {
                if (topologyFile.getName().equals(maxTopology)) {
                    continue;
                }
                System.out.println(runNumber + ": " + topologyFile.getName());
                //update topology
                {
                    Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                            new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), topologyFile.getPath(),
                            parameters, new ArrayList<Topology>()).get(0);
                    for (Switch aSwitch : topology.getSwitches()) {
                        simTopology.getSwitchMap().get(aSwitch.getId()).fillParam(aSwitch);
                    }
                }

                runForTopology(simTopology, parameters, false, flows, outputFolder, placer,
                        new File(outputFolder + "/assignment/assignment_" + topologyFile.getName()), partitions);
                runNumber++;
            }
        }
        System.out.println("finished");
    }


    public static boolean runForTopology(Topology topology,
                                         Map<String, Object> parameters, boolean firstRun, Collection<Flow> flows,
                                         String outputFolder, Placer placer, File assignmentFile, List<Partition> partitions) throws Exception {
        boolean successful = false;

        try {
            final Assignment assignment = doAssignment(topology, parameters, firstRun, flows, outputFolder, placer, assignmentFile, partitions);

            //switchMemorySaveFile.run();
            successful = true;
            //new SaveFileProcessor<String>(new PlacementToStringProcessor(placeProcessor), new File(outputFolder + "/switchmemory/" + runNumber + ".txt"), false).run();
        } catch (NoAssignmentFoundException e) {
            System.out.println();
        }

        writeSolutionCSV(parameters, firstRun, outputFolder, successful);
        topology.reset();
        return successful;
    }

    public static void writeSolutionCSV(Map<String, Object> parameters, boolean firstRun, String outputFolder, boolean successful) throws Exception {
        Statistics solutionExistenceStat = new Statistics();
        solutionExistenceStat.setParameters(parameters);
        solutionExistenceStat.addStat(SOLUTION_METRIC, successful ? 1 : 0);
        new SaveFileProcessor<String>(Statistics.csvStatistics(parameters.keySet(), Statistics.categorize(parameters.keySet(), Collections.singleton(solutionExistenceStat)),
                Arrays.asList(SOLUTION_METRIC), false, firstRun),
                new File(outputFolder + "/solution.csv"), !firstRun).run();
    }

    protected static Assignment doAssignment(Topology topology,
                                             Map<String, Object> parameters, boolean firstRun, Collection<Flow> flows, String outputFolder,
                                             Placer placer, File assignmentFile, List<Partition> partitions) throws Exception {
        long start = System.currentTimeMillis();
        final Assignment assignment = placer.place(topology, partitions);
        WriterSerializableUtil.writeFile(assignment, assignmentFile, false, parameters);
        {
            //System.out.println("place: " + (System.currentTimeMillis() - start));
            //new SetForwardingRulesProcessor().process(assignment, classifiedFlows, topology);
            //System.out.println("swfw: " + (System.currentTimeMillis() - start));
            RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
            runFlowsOnNetworkProcessor2.process(topology, flows);
            //System.out.println("runflow: " + (System.currentTimeMillis() - start));
            PrintWriter runFlowStatsWriter = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder + "/runflows.txt", !firstRun)), true);
            runFlowStatsWriter.println(Statistics.getParameterLine(parameters));
            runFlowsOnNetworkProcessor2.print(runFlowStatsWriter);
            runFlowStatsWriter.println();

            Statistics placementStats = topology.getStat(parameters);
            placementStats.joinStats(placer.getStats(parameters));
            new SaveFileProcessor<String>(Statistics.csvStatistics(parameters.keySet(), Statistics.categorize(parameters.keySet(), Collections.singleton(placementStats)),
                    placementStats.getStatNames(), true, firstRun),
                    new File(outputFolder + "/placement.csv"), !firstRun).run();
        }
        return assignment;
    }

    /* public static void fillFromAssignment(Assignment assignment, Topology topology, Map<Partition, Map<Switch, Rule>> forwardingRules,
                                          Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) throws Switch.InfeasibleStateException {
        assignment.updateForwardingRules(forwardingRules);
        Map<Switch, Set<Rule>> switchRulesMap = new HashMap<>(topology.getSwitches().size(), 1);
        Map<Switch, Integer> switchNewFlows = new HashMap<>(topology.getSwitches().size(), 1);
        for (Switch aSwitch : topology.getSwitches()) {
            switchRulesMap.put(aSwitch, aSwitch.getState().getRules());
            switchNewFlows.put(aSwitch, 0);
        }
        for (Map.Entry<Partition, Switch> entry1 : assignment.getPlacement().entrySet()) {
            final Partition partition = entry1.getKey();
            final Switch host = entry1.getValue();
            switchRulesMap.get(host).addAll(partition.getRules());
            for (Map.Entry<Switch, Rule> entry2 : forwardingRules.get(partition).entrySet()) {
                final Switch src = entry2.getKey();
                if (!src.equals(host)) {
                    switchRulesMap.get(src).add(entry2.getValue());
                }
            }
            for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
                final Collection<Flow> flows = entry.getValue();
                switchNewFlows.put(host, switchNewFlows.get(host) + flows.size());
                for (Flow flow : flows) {
                    final Switch source = flow.getSource();
                    final Switch destination = flow.getDestination();
                    if (!host.equals(source)) {
                        switchNewFlows.put(source, switchNewFlows.get(source) + 1);
                    }
                    if (entry.getKey().getAction().doAction(flow) != null &&
                            !destination.equals(host)) {
                        //add accept flows to the source again
                        switchNewFlows.put(destination, switchNewFlows.get(destination) + 1);
                    }
                }
            }

            for (Map.Entry<Switch, Set<Rule>> entry : switchRulesMap.entrySet()) {
                entry.getKey().isFeasible(entry.getValue(), switchNewFlows.get(entry.getKey()), true, true);
            }
        }
    }*/

    public static void resetToMatrixRuleSet(UnifiedPartitionFactory partitionFactory, List<Partition> partitions,
                                            Topology simTopology, Map<Partition, Map<Switch, Rule>> forwardingRules) throws Switch.InfeasibleStateException {
        // reset rulesets to matrix sets
        /// first find the ruleset
        ArrayList<Rule> fwRules = new ArrayList<Rule>(partitions.size());
        for (Map<Switch, Rule> v : forwardingRules.values()) {
            for (Rule rule : v.values()) {
                fwRules.add(rule);
            }
        }
        Collections.sort(fwRules, new Comparator<Rule>() {
            @Override
            public int compare(Rule o1, Rule o2) {
                return o1.getId() - o2.getId();
            }
        });
        List<Rule> allRules = new ArrayList<Rule>(partitions.size() + partitionFactory.getRulesSize());
        allRules.addAll(partitionFactory.getRules());
        allRules.addAll(fwRules);
        fwRules.clear();
        MatrixRuleSet.setRuleSet(allRules);
        for (Partition partition : partitions) {
            final MatrixRuleSet partitionRules = new MatrixRuleSet();
            partitionRules.addAll(partition.getRules());
            partition.setRules(partitionRules);
        }
        CollectionPool<Set<Rule>> ruleSetPool = new CollectionPool<Set<Rule>>(new MatrixRuleSet());
        CollectionPool<Set<Long>> wildcardPool = new CollectionPool<Set<Long>>(new HashSet<Long>());
        simTopology.initHelpers(ruleSetPool, wildcardPool);
        for (Switch aSwitch : simTopology.getSwitches()) {
            simTopology.getHelper(aSwitch).resetRuleCollections(aSwitch);
        }
        simTopology.getHelper(simTopology.getControllerSwitch()).resetRuleCollections(simTopology.getControllerSwitch());

        Runtime.getRuntime().gc();
    }
}
