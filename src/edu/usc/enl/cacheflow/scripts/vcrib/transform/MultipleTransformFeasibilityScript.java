package edu.usc.enl.cacheflow.scripts.vcrib.transform;

import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.algorithms.PostPlacer;
import edu.usc.enl.cacheflow.algorithms.feasibility.general.FeasiblePlacer2;
import edu.usc.enl.cacheflow.algorithms.migration.rmigration3.RMigrateVMStartPartition3;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.partition.UnifiedPartitionWriter;
import edu.usc.enl.cacheflow.processor.partition.transform.*;
import edu.usc.enl.cacheflow.processor.statistics.PartitionStatisticsProcessor;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
import edu.usc.enl.cacheflow.scripts.stats.LoadDistributionStats;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePostPlaceScriptCluster2;
import edu.usc.enl.cacheflow.scripts.vmstart.GetShortestPathLoad;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.CustomRandomGenerator;
import edu.usc.enl.cacheflow.util.LaunchAJVM;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/29/12
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class MultipleTransformFeasibilityScript {
    protected String partitionFile;
    protected String partitionsOutputFolder;
    protected String outputStatsFolder;
    protected HashMap<String, Object> parameters;
    protected Topology vmStartTopology;
    protected Topology simTopology;
    protected List<Flow> flows;
    protected UnifiedPartitionFactory partitionFactory;
    protected List<Partition> partitions;
    protected Map<Switch, Collection<Partition>> sourcePartitions;
    protected Placer placer;
    protected Map<Partition, Map<Switch, Rule>> forwardingRules;
    protected String vmStartTopologyFile;
    protected File vmsFile;
    protected String flowFile;

    public static void main(String[] args) throws Exception, NoAssignmentFoundException {
        new MultipleTransformFeasibilityScript().run(args);
    }

    public void run(String[] args) throws Exception, NoAssignmentFoundException {
        init(args);

        //int[] changeNums = new int[]{ 2000, 8000, 32000,48000, 64000,96000};
        //int[] changeNums = new int[]{8000,16000,32000,64000};

        int resolution = Integer.parseInt(args[10]);
//        10*(20*128);
        int runs = Integer.parseInt(args[11]);
        List<Integer> changeNums = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            changeNums.add((i + 1) * resolution);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //create transformer
        PartitionTransformer transformer;
        if (args[9].equalsIgnoreCase("break")) {
            transformer = new BreakPartitionTransformer(true, false);
        } else if (args[9].equalsIgnoreCase("extend")) {
            transformer =
                    new ExtendRulesPartitionTransformer(true, true,false);
        } else if (args[9].equalsIgnoreCase("add")) {
            transformer = new AddRemoveRandomRulesPartitionTransformer(
                    new CustomRandomGenerator<Boolean>(new Boolean[]{Boolean.TRUE, Boolean.FALSE}, new double[]{1, 0}),
                    new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1}),
                    new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1}),
                    true, false);
        } else if (args[9].equalsIgnoreCase("remove")) {
            transformer = new AddRemoveRandomRulesPartitionTransformer(
                    new CustomRandomGenerator<Boolean>(new Boolean[]{Boolean.TRUE, Boolean.FALSE}, new double[]{0, 1}),
                    new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1}),
                    new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1}),
                    true, false);
        } else if (args[9].equalsIgnoreCase("removeall")) {
            //create removeall transformer
            Long[] removeSizes = new Long[31];
            double[] removeProbs = new double[31];
            for (int i = 0; i < 31; i++) {
                removeSizes[i] = 1l << (i + 1);
                removeProbs[i] = 1;
            }

            transformer =
                    new AddRemoveRandomRulesPartitionTransformer(
                            new CustomRandomGenerator<Boolean>(new Boolean[]{Boolean.TRUE, Boolean.FALSE}, new double[]{0, 1}),
                            new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1}),
                            new CustomRandomGenerator<Long>(removeSizes, removeProbs),
                            true, false);
        } else if (args[9].equalsIgnoreCase("addall")) {
            //create removeall transformer
            Long[] removeSizes = new Long[31];
            double[] removeProbs = new double[31];
            for (int i = 0; i < 31; i++) {
                removeSizes[i] = 1l << (i + 1);
                removeProbs[i] = 1;
            }

            transformer =
                    new AddRemoveRandomRulesPartitionTransformer(
                            new CustomRandomGenerator<Boolean>(new Boolean[]{Boolean.TRUE, Boolean.FALSE}, new double[]{1, 0}),
                            new CustomRandomGenerator<Long>(removeSizes, removeProbs),
                            new CustomRandomGenerator<Long>(new Long[]{1l}, new double[]{1}),
                            true, false);
        } else {
            throw new Exception("command not found");
        }

        int changesSum = 0;
        //for each value of changenum
        boolean incompleteRun = false;
        for (int changeNum2 : changeNums) {
            System.out.println(changeNum2);
            int changeNum = changeNum2 - changesSum;
            incompleteRun = runChanges(transformer, incompleteRun, changeNum2, changeNum);

            changesSum = changeNum2;
            if (incompleteRun) {
                break;
            }
        }
    }

    private boolean runChanges(PartitionTransformer transformer, boolean incompleteRun, int changeNum2, int changeNum) throws Exception {
        //transform partitions
        try {
            transformer.transform(Util.random, changeNum,
                    partitions, sourcePartitions, vmStartTopology);
        } catch (IncompleteTransformException e) {
            System.out.println(e.getMessage());
            incompleteRun = true;
        }

        //save partitions
        String currentOutputFolder = outputStatsFolder + "/" + changeNum2;
        new File(currentOutputFolder).mkdirs();

        String partitionFileName = new File(partitionFile).getName();
        String newPartitionFile = savePartitions(partitionsOutputFolder + "/" + changeNum2, parameters, partitions, partitionFileName);

        //compute similarity
        String partitionStatsFileName = savePartitionStats(changeNum2, currentOutputFolder, newPartitionFile);//need to load from file as the id of rules are not consecutive any more


        //save distribution of load on machines
        double maxLoad = LoadDistributionStats.serverLoadDistribution(currentOutputFolder, vmStartTopology, partitions, sourcePartitions, partitionFileName);

        //load simulation topology
        simTopology.reset();
        //run feasibility test algorithm
        boolean placementFound =
                //resourceFeasibility(partitionFileName, currentOutputFolder);
                trafficFeasibility(currentOutputFolder, 10, newPartitionFile);
        System.out.println(placementFound ? "Placement found" : "Placement not found");

        //if maximum partition is >machine capacity or maximum loaded machine is < machine capacity
        double maxSize = Statistics.extractStatFromFile(partitionStatsFileName, PartitionStatisticsProcessor.MAX_SIZE_OF_PARTITIONS_STAT, 1);
//        if (maxSize > 4096 || maxLoad <= 4096) {
//            return true;
//        }

        return incompleteRun;
    }

    protected String savePartitionStats(int changeNum2, String currentOutputFolder, String partitionFile) throws Exception {
        final Statistics stats = new PartitionStatisticsProcessor(
                Util.loadFile(
                        new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()),
                        partitionFile,
                        parameters,
                        new LinkedList<Partition>()),
                parameters).run();
        final ArrayList<String> parameterNames = new ArrayList<String>(parameters.keySet());
        String outputFileName = currentOutputFolder + "/Partition_stats" + changeNum2 + ".csv";
        Util.writeFile(Statistics.csvStatistics(parameterNames,
                Statistics.categorize(parameterNames, Collections.singleton(stats)), stats.getStatNames(),
                true, true), new File(outputFileName), false);
        return outputFileName;
    }

    protected void init(String[] args) throws IOException, UnalignedRangeException {
        Util.setRandom(Integer.parseInt(args[0]));
        Util.threadNum = Integer.parseInt(args[1]);
        vmStartTopologyFile = args[2];
        String simTopologyFile = args[3];
        partitionFile = args[4];
        vmsFile = new File(args[5]);
        flowFile = args[6];
        partitionsOutputFolder = args[7];
        outputStatsFolder = args[8];

        //load topology
        parameters = new HashMap<>();
        vmStartTopology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.createDefaultAggregator(), new HashSet<Rule>()),
                vmStartTopologyFile, parameters, new LinkedList<Topology>()).get(0);
        simTopology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.createDefaultAggregator(), new HashSet<Rule>()),
                simTopologyFile, parameters, new LinkedList<Topology>()).get(0);

        //load flows
        flows = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                simTopology), flowFile, parameters, new LinkedList<Flow>(), "flow\\..*");

        //load vm assignment
        Map<Long, Switch> vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), vmStartTopology), vmsFile.getPath(),
                parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
        //load partitions
        partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), false, new HashSet<Rule>());
        partitions = Util.loadFileFilterParam(partitionFactory,
                partitionFile, parameters, new LinkedList<Partition>(), "(rule\\..*|partition\\..*)");

        //create sourcepartitions
        sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(partitions, vmSource);

        //clear feasibilty checker
        forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, simTopology, sourcePartitions);
        placer = new FeasiblePlacer2(false, forwardingRules,
                Util.threadNum, sourcePartitions);

        //init topologies note that it is not matrix! just hashset because the ruleset changes alot
        //I CREATE HELPERS HERE AS WE ONLY USE THIS FOR MEMORY NOT CPU. OTHERWISE I NEED TO CLASSIFY PER RUN
        {
            Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows = new ThreadTwoLevelTrafficProcessor(
                    new OVSClassifier(),
                    new OVSClassifier(), Util.threadNum).classify(flows, partitions);

            vmStartTopology.createHelpers(partitions, forwardingRules, classifiedFlows);
            vmStartTopology.initHelpers(new CollectionPool<Set<Rule>>(new HashSet<Rule>()), new CollectionPool<Set<Long>>(new HashSet<Long>()));
            simTopology.createHelpers(partitions, forwardingRules, classifiedFlows);
            simTopology.initHelpers(new CollectionPool<Set<Rule>>(new HashSet<Rule>()), new CollectionPool<Set<Long>>(new HashSet<Long>()));
        }
    }

    protected String savePartitions(String partitionsOutputFolder, HashMap<String, Object> parameters,
                                    List<Partition> partitions, String partitionFileName) throws IOException {
        new File(partitionsOutputFolder).mkdirs();
        String outputFile = partitionsOutputFolder + "/" + partitionFileName;
        PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
        new UnifiedPartitionWriter().write(partitions, pw, parameters);
        pw.close();
        return outputFile;
    }

    public static boolean checkPartitionsRule(List<Partition> partitions, int srcIPIndex) {
        Set<Rule> rules = new HashSet<>();
        TreeMap<Long, Partition> srcIPPartition = new TreeMap<>();
        for (Partition partition : partitions) {
            for (Rule rule : partition.getRules()) {
                if (!partition.getProperty(srcIPIndex).hasIntersect(rule.getProperty(srcIPIndex))) {
                    return false;
                }
                rules.add(rule);
            }
            srcIPPartition.put(partition.getProperty(srcIPIndex).getStart(), partition);
        }

        for (Rule rule : rules) {
            NavigableMap<Long, Partition> subMap = srcIPPartition.subMap(rule.getProperty(srcIPIndex).getStart(), true, rule.getProperty(srcIPIndex).getEnd(), true);
            for (Partition partition : subMap.values()) {
                if (!partition.getRules().contains(rule)) {
                    return false;
                }
            }
        }

        return true;
    }

    protected boolean trafficFeasibility(String currentOutputFolder, double trafficBound, String newPartitionFile) throws Exception {
        //classify flows.
        Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows = new ThreadTwoLevelTrafficProcessor(
                new OVSClassifier(),
                new OVSClassifier(), Util.threadNum).classify(flows, partitions);
        //recreate helpers! NO NEED ALL SWITCHES ARE MEMORY

        vmStartTopology.setRuleFlowMap(classifiedFlows);
        simTopology.setRuleFlowMap(classifiedFlows);
        vmStartTopology.reset();

        double vmStartTraffic = 0;
        {

            //run vmstart
            String statfile = currentOutputFolder + "/vmstart/nsdi_classbench_shortestpath.csv";
            String runFlowStatsFile = currentOutputFolder + "/vmstart/runflow_stats.txt";

            boolean returnValue = new LaunchAJVM().run(GetShortestPathLoad.class, "-Xmx10g -Xmn1g -Xms2g",
                    Util.threadNum + " " +
                            newPartitionFile + " " +
                            vmStartTopologyFile + " " +
                            flowFile + " " +
                            vmsFile + " " +
                            statfile + " " +
                            runFlowStatsFile + " " +
                            "false");
            if (!returnValue) {
                System.out.println("Error in vmstart");
                System.exit(1);
            }

            vmStartTraffic = Statistics.extractStatFromFile(statfile, Topology.TRAFFIC_STAT, 1);
        }


        final int initDownSteps = 1;
        final double alphaDownSteps = 0.5;
        int timeBudget = 0;
        Map<Partition, Long> minTraffic = MultiplePostPlaceScriptCluster2.getMinOverhead(simTopology, classifiedFlows);
        PostPlacer postPlacer = new RMigrateVMStartPartition3(
                simTopology, minTraffic, sourcePartitions, forwardingRules, Util.threadNum,
                100, 1, initDownSteps, alphaDownSteps, timeBudget);
        parameters.put("postPlacement1." + postPlacer + ".initDownSteps", initDownSteps);
        parameters.put("postPlacement1." + postPlacer + ".alphaDownSteps", alphaDownSteps);
        parameters.put("postPlacement1." + postPlacer + ".timeBudget", timeBudget);

        parameters.put("postPlacement1.alg", postPlacer);

        List<PostPlacer> postPlacers = Arrays.asList(postPlacer);
        new File(currentOutputFolder + "/placement").mkdirs();
        parameters.put("placement.alg", placer);
        boolean resourceFeasible = false;
        try {
            resourceFeasible = MultiplePostPlaceScriptCluster2.runForTopologyN(simTopology, parameters, true, flows, currentOutputFolder, placer,
                    currentOutputFolder + "/placement", sourcePartitions, forwardingRules, postPlacers, "topology");
        } catch (NoAssignmentFoundException e) {
            resourceFeasible = false;
        }
        if (!resourceFeasible) {
            return false;
        }

        {
            //load traffic value from stats of the first postplacer.
            //open file
            String statsFile = currentOutputFolder + "/0_postplacement/postplacement.csv";
            double value = Statistics.extractStatFromFile(statsFile, Topology.TRAFFIC_STAT, 1);
            return value <= vmStartTraffic * (1 + trafficBound / 100);
        }
    }

    protected boolean resourceFeasibility(String partitionFileName, String currentOutputFolder) throws Exception {
        return MultiplePlacementScript.runForTopology(simTopology, parameters, true, flows,
                currentOutputFolder, placer, new File(currentOutputFolder + "assignment_" + partitionFileName), partitions);
    }
}
