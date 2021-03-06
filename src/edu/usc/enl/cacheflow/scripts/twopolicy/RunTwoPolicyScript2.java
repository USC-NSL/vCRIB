package edu.usc.enl.cacheflow.scripts.twopolicy;

import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchhelper.MemorySwitchHelper;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.file.SaveFileProcessor;
import edu.usc.enl.cacheflow.processor.network.RunFlowsOnNetworkProcessor2;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePostPlaceScriptCluster2;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.logging.Level;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 9/15/12
 * Time: 4:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class RunTwoPolicyScript2 {
    public static void main(String[] args) throws Exception, UnalignedRangeException, Switch.InfeasibleStateException {
        Util.threadNum = Integer.parseInt(args[0]);
        int randomSeedIndex = Integer.parseInt(args[1]);
        String maxTopology = args[2];
        File topologyFolder = new File(args[3]);
        File flowFile = new File(args[4]);
        File vmFile = new File(args[5]);
        File partitionFile1 = new File(args[6]);
        File partitionFile2 = new File(args[7]);
        String assignmentFolder1 = args[8];
        String assignmentFolder2 = args[9];
        String outputFolder = args[10];

        Util.setRandom(randomSeedIndex);
        Util.logger.setLevel(Level.WARNING);
        new File(outputFolder).mkdirs();
        Map<String, Object> parameters = new HashMap<String, Object>();

        long start = System.currentTimeMillis();


        String maxTopologyPath = topologyFolder.getPath() + "/" + maxTopology;
        Topology simTopology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), maxTopologyPath, parameters, new ArrayList<Topology>()).get(0);
        List<Flow> flows = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                simTopology), flowFile.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");

        System.out.println("load flows " + (System.currentTimeMillis() - start));

        ///////////////////////////////////////////////////////////////////////////////////////
        //load partitions 1
        {
            UnifiedPartitionFactory partitionFactory1 = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
            List<Partition> partitions = Util.loadFileFilterParam(partitionFactory1, partitionFile1.getPath(), parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");
            Map<Long, Switch> vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), simTopology), vmFile.getPath(),
                    parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
            final Map<Switch, Collection<Partition>> sourcePartitions1 = MultiplePostPlaceScriptCluster2.getSourcePartitions(partitions, vmSource);

            //just to keep helper happy!!!
            Map<Partition, Map<Switch, Rule>> forwardingRules1 = MultiplePlacementScript.createForwardingRules(partitions, simTopology, sourcePartitions1);

            CollectionPool<Set<Rule>> ruleSetPool = new CollectionPool<Set<Rule>>(new MatrixRuleSet());
            final MemorySwitchHelper helper = new MemorySwitchHelper(forwardingRules1);
            helper.init();
            helper.setRuleSetPool(ruleSetPool);
            //load on servers
            for (Map.Entry<Switch, Collection<Partition>> entry : sourcePartitions1.entrySet()) {
                MemorySwitch aSwitch = (MemorySwitch) entry.getKey();
                for (Partition partition : entry.getValue()) {
                    helper.isAddFeasible(aSwitch, partition, Collections.singleton(((Switch) aSwitch)), true, true);
                }
            }
        }

        //load partitions2
        {
            UnifiedPartitionFactory partitionFactory2 = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
            List<Partition> partitions = Util.loadFileFilterParam(partitionFactory2, partitionFile2.getPath(), parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");
            Map<Long, Switch> vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), simTopology), vmFile.getPath(),
                    parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
            final Map<Switch, Collection<Partition>> sourcePartitions2 = MultiplePostPlaceScriptCluster2.getSourcePartitions(partitions, vmSource);
            Map<Partition, Map<Switch, Rule>> forwardingRules2 = MultiplePlacementScript.createForwardingRules(partitions, simTopology, sourcePartitions2);

            CollectionPool<Set<Rule>> ruleSetPool = new CollectionPool<Set<Rule>>(new MatrixRuleSet());
            final MemorySwitchHelper helper = new MemorySwitchHelper(forwardingRules2);
            helper.init();
            helper.setRuleSetPool(ruleSetPool);
            for (Map.Entry<Switch, Collection<Partition>> entry : sourcePartitions2.entrySet()) {
                MemorySwitch aSwitch = (MemorySwitch) entry.getKey();
                Switch torSwitch = aSwitch.getLinks().get(0).getEnd();
                for (Partition partition : entry.getValue()) {
                    helper.isAddFeasible((MemorySwitch) torSwitch, partition, Collections.singleton(((Switch) aSwitch)), true, true);
                }
            }
        }

        RunFlowsOnNetworkProcessor2 runFlowsOnNetworkProcessor2 = new RunFlowsOnNetworkProcessor2();
        runFlowsOnNetworkProcessor2.process(simTopology, flows);
        System.out.println("runflow: " + (System.currentTimeMillis() - start));
        PrintWriter runFlowStatsWriter = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder + "/runflows.txt", false)), true);
        runFlowStatsWriter.println(Statistics.getParameterLine(parameters));
        runFlowsOnNetworkProcessor2.print(runFlowStatsWriter);
        runFlowStatsWriter.println();

        Statistics placementStats = simTopology.getStat(parameters);
        new SaveFileProcessor<String>(Statistics.csvStatistics(parameters.keySet(), Statistics.categorize(parameters.keySet(), Collections.singleton(placementStats)),
                placementStats.getStatNames(), true, true),
                new File(outputFolder + "/placement.csv"), false).run();


        System.out.println("load partition " + (System.currentTimeMillis() - start));

    }

    private static Map<Switch, Collection<Partition>> getSourcePartitions(List<Flow> flows, List<Partition> partitions1, List<Partition> partitions2) {
        Map<Long, Partition> partition1Map = new HashMap<>(partitions1.size());
        int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        for (Partition partition : partitions1) {
            partition1Map.put(partition.getProperty(srcIPIndex).getStart(), partition);
        }
        Map<Long, Partition> partition2Map = new HashMap<>(partitions2.size());
        for (Partition partition : partitions2) {
            partition2Map.put(partition.getProperty(srcIPIndex).getStart(), partition);
        }
        Map<Switch, Collection<Partition>> output = new HashMap<>();
        for (Flow flow : flows) {
            Switch source = flow.getSource();
            Collection<Partition> partitions = output.get(source);
            if (partitions == null) {
                partitions = new HashSet<Partition>();
                output.put(source, partitions);
            }
            partitions.add(partition1Map.get(flow.getProperty(srcIPIndex)));
            partitions.add(partition2Map.get(flow.getProperty(srcIPIndex)));
        }
        return output;
    }

    public static class TwoAssignmentPlacer extends Placer {
        private final int firstSetSize;
        private final Map<String, Object> parameters;
        private final String inputFolder1;
        private final String inputFolder2;
        private final List<Partition> partitions;

        public TwoAssignmentPlacer(boolean checkLinks, Map<Partition, Map<Switch, Rule>> forwardingRules,
                                   Map<Switch, Collection<Partition>> sourcePartitions,
                                   int firstSetSize, Map<String, Object> parameters, String inputFolder1, String inputFolder2, List<Partition> partitions) {
            super(checkLinks, forwardingRules, sourcePartitions, 1);
            this.firstSetSize = firstSetSize;
            this.parameters = parameters;
            this.inputFolder1 = inputFolder1;
            this.inputFolder2 = inputFolder2;
            this.partitions = partitions;
        }

        @Override
        public Collection<Switch> getLastAvailableSwitches() {
            return topology.getSwitches();
        }

        @Override
        public Assignment place2(Topology topology, Collection<Partition> partitions) throws NoAssignmentFoundException {
            Map<Integer, Partition> originalID = new HashMap<>();
            for (Partition partition : this.partitions) {
                originalID.put(partition.getId(), partition);
            }
            int id = 1;
            int index = 0;
            for (Partition partition : this.partitions) {
                partition.setId(id++);
                index++;
                if (index == firstSetSize) {
                    id = 1;
                }
            }

            List<Partition> partitions1 = this.partitions.subList(0, firstSetSize);
            parameters.put("topology.edgeMemory", ((Number) parameters.get("topology.edgeMemory")).longValue() / 2);
            parameters.put("topology.internalMemory", ((Number) parameters.get("topology.internalMemory")).longValue() / 2);
            Assignment assignment1 = AssignmentFactory.LoadPlacer.loadAssignment(topology,
                    new AssignmentFactory(new FileFactory.EndOfFileCondition(), topology, partitions1), inputFolder1, partitions1, parameters);
            List<Partition> partitions2 = this.partitions.subList(firstSetSize, this.partitions.size());
            Assignment assignment2 = AssignmentFactory.LoadPlacer.loadAssignment(topology,
                    new AssignmentFactory(new FileFactory.EndOfFileCondition(), topology, partitions2), inputFolder2, partitions2, parameters);
            Map<Partition, Switch> placement = new IdentityHashMap<>(assignment1.getPlacement());
            placement.putAll(assignment2.getPlacement());
            for (Map.Entry<Integer, Partition> entry : originalID.entrySet()) {
                entry.getValue().setId(entry.getKey());
            }
            parameters.put("topology.edgeMemory", ((Number) parameters.get("topology.edgeMemory")).longValue() * 2);
            parameters.put("topology.internalMemory", ((Number) parameters.get("topology.internalMemory")).longValue() * 2);
            final Assignment currentAssignment = new Assignment(placement);
            {
                CollectionPool<Set<Rule>> ruleSetPool = new CollectionPool<Set<Rule>>(new MatrixRuleSet());
                final MemorySwitchHelper helper = new MemorySwitchHelper(forwardingRules);
                helper.init();
                helper.setRuleSetPool(ruleSetPool);
                try {
                    final Iterator<Switch> itr = topology.getSwitches().iterator();
                    Thread[] threads = new Thread[Util.threadNum];
                    for (int i = 0; i < threads.length; i++) {
                        threads[i] = new Thread() {
                            @Override
                            public void run() {
                                while (true) {
                                    Switch next;
                                    synchronized (itr) {
                                        if (itr.hasNext()) {
                                            next = itr.next();
                                        } else {
                                            return;
                                        }
                                    }
                                    try {
                                        loadOnSwitch(currentAssignment, helper, next);
                                    } catch (Switch.InfeasibleStateException e) {
                                        e.printStackTrace();
                                        return;
                                    } catch (NoAssignmentFoundException e) {
                                        e.printStackTrace();
                                        return;
                                    }
                                }
                            }
                        };
                    }
                    Util.runThreads(threads);
                    /*for (Switch aSwitch : topology.getSwitches()) {
                        loadOnSwitch(currentAssignment, helper, aSwitch);
                    }*/
                    Switch.ControllerSwitchHelper controllerSwitchHelper = new Switch.ControllerSwitchHelper();
                    controllerSwitchHelper.setRuleSetPool(ruleSetPool);
                    controllerSwitchHelper.init();
                    controllerSwitchHelper.resetRuleCollections(topology.getControllerSwitch());
                } catch (Switch.InfeasibleStateException e) {
                    throw new NoAssignmentFoundException(e);
                }


                currentAssignment.updateForwardingRules(forwardingRules);
            }
            return currentAssignment;

        }

        private void loadOnSwitch(Assignment currentAssignment, MemorySwitchHelper helper, Switch aSwitch) throws Switch.InfeasibleStateException, NoAssignmentFoundException {
            helper.resetRuleCollections((MemorySwitch) aSwitch);
            helper.initToNotOnSrc((MemorySwitch) aSwitch, sourcePartitions.get(aSwitch), true);
            final Set<Partition> rPlacement = currentAssignment.getRplacement().get(aSwitch);
            if (rPlacement != null) {
                try {
                    helper.isAddMultipleFeasible((MemorySwitch) aSwitch, rPlacement, forwardingRules, true);
                } catch (Switch.InfeasibleStateException e) {
                    throw new NoAssignmentFoundException(e);
                }
            }
        }
    }

    public static void resetToMatrixRuleSet(List<Rule> rules1, List<Rule> rules2, List<Partition> partitions,
                                            Topology simTopology, Map<Partition, Map<Switch, Rule>> forwardingRules
    ) throws Switch.InfeasibleStateException {
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
        List<Rule> allRules = new ArrayList<Rule>(partitions.size() + rules1.size() + rules2.size());

        int maxPriority = 206873;
        Rule defaultRule1 = null;
        for (Iterator<Rule> iterator = rules1.iterator(); iterator.hasNext(); ) {
            Rule rule = iterator.next();
            if (rule.getPriority() == maxPriority) {
                iterator.remove();
                break;
            }
        }
        allRules.addAll(rules1);
        allRules.addAll(rules2);
        allRules.addAll(fwRules);
        fwRules.clear();
        MatrixRuleSet.setRuleSet(allRules);
        for (Partition partition : partitions) {
            final MatrixRuleSet partitionRules = new MatrixRuleSet();
            partitionRules.addAll(partition.getRules());
            partition.setRules(partitionRules);
        }
        if (simTopology.hasHelper()) {
            CollectionPool<Set<Rule>> ruleSetPool = new CollectionPool<Set<Rule>>(new MatrixRuleSet());
            CollectionPool<Set<Long>> wildcardPool = new CollectionPool<Set<Long>>(new HashSet<Long>());
            simTopology.initHelpers(ruleSetPool, wildcardPool);
            for (Switch aSwitch : simTopology.getSwitches()) {
                simTopology.getHelper(aSwitch).resetRuleCollections(aSwitch);
            }
            simTopology.getHelper(simTopology.getControllerSwitch()).resetRuleCollections(simTopology.getControllerSwitch());
        }

        Runtime.getRuntime().gc();
    }
}
