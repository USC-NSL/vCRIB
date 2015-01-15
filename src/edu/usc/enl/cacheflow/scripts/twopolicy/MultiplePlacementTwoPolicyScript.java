package edu.usc.enl.cacheflow.scripts.twopolicy;

import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.algorithms.feasibility.general.FeasiblePlacer2;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePostPlaceScriptCluster2;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.util.*;
import java.util.logging.Level;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/1/12
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class MultiplePlacementTwoPolicyScript {

    public static final String SOLUTION_METRIC = "solution";

    public static void main(String[] args) throws Exception {
        Util.threadNum = Integer.parseInt(args[0]);
        int randomSeedIndex = Integer.parseInt(args[1]);
        String maxTopology = args[2];
        File topologyFolder = new File(args[3]);
        File flowFile = new File(args[4]);
        File partitionFile = new File(args[5]);
        String outputFolder = args[6];

        Map<String, Object> parameters = new HashMap<String, Object>();
        //run(flowsFile, partitionFile, topologyFolder, parameters, maxTopology, randomSeedIndex, outputFolder);
        //laundry stuff
        Util.setRandom(randomSeedIndex);
        Util.logger.setLevel(Level.WARNING);
        new File(outputFolder).mkdirs();

        //load data
        long start = System.currentTimeMillis();

        final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
        List<Partition> partitions = Util.loadFileFilterParam(partitionFactory, partitionFile.getPath(), parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");

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

        final Map<Switch, Collection<Partition>> sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(classifiedFlows);
        Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, simTopology, sourcePartitions);

        System.out.println("forwarding rules " + (System.currentTimeMillis() - start));

        simTopology.createHelpers(partitions, forwardingRules, classifiedFlows);
        simTopology.setRuleFlowMap(classifiedFlows);

        System.out.println("helpers " + (System.currentTimeMillis() - start));

        UnifiedPartitionFactory partitionFactoryBase = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
        Util.loadFileFilterParam(partitionFactoryBase, "input\\nsdi\\classbenchpartition\\vmstart\\imcsplit\\1\\20480_classbench_131072_2.txt", parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");
        MultiplePlacementTwoPolicyScript.resetToMatrixRuleSet(partitionFactory, partitions, simTopology, forwardingRules,
                partitionFactoryBase);

        System.out.println("reset rulesets " + (System.currentTimeMillis() - start));

        Placer placer = new FeasiblePlacer2(false, forwardingRules, Util.threadNum, sourcePartitions);


        //Placer placer = new Assigner(switchSelection, partitionSorter, 100, false, forwardingRules);
//        parameters.put("placement." + placer + ".partitionSelection", partitionSorter);
//        parameters.put("placement." + placer + ".switchSelection", switchSelection);

        parameters.put("placement.alg", placer);

        //run for max configuration
        MultiplePlacementScript.runForTopologies(maxTopology, topologyFolder, outputFolder, parameters, simTopology, flows, placer, partitions);

    }

    public static void resetToMatrixRuleSet(UnifiedPartitionFactory partitionFactory, List<Partition> partitions,
                                            Topology simTopology, Map<Partition, Map<Switch, Rule>> forwardingRules,
                                            UnifiedPartitionFactory partitionFactoryBase) throws Switch.InfeasibleStateException {
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
        if (partitionFactoryBase != null) {
            allRules.addAll(partitionFactoryBase.getRules());
        }
        allRules.addAll(partitionFactory.getRules());
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
