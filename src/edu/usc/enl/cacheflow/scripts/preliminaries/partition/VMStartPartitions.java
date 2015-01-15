package edu.usc.enl.cacheflow.scripts.preliminaries.partition;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.sourcevm.SourceVMPartition;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.IPAssigner;
import edu.usc.enl.cacheflow.processor.partition.UnifiedPartitionWriter;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
import edu.usc.enl.cacheflow.util.Util;
import org.apache.commons.collections15.map.LinkedMap;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/6/12
 * Time: 8:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class VMStartPartitions {
    public static void main(String[] args) throws IOException {
        String parentFolder = "input/nsdi";
        ///////////////////////////////////////////// PARAMETERS
//        String inputRulesFolder = parentFolder + "/rulestenant";
//        String inputFlowsFolder = parentFolder + "/flowstenant";
//        String outputFolder = parentFolder + "/partitiontenant/vmstart";
//        String statsOutputFolder = parentFolder + "/partitiontenant/vmstartstat";
//        String topologyFile = parentFolder + "/topologylm/memory/tree_4096_1024.txt";
//        String paramSets = "";
//        String ruleSize = "";

//        String inputRulesFolder = parentFolder + "/classbenchrules/split3/1";
//        String inputFlowsFolder = parentFolder + "/classbenchflows/imcsplit3/1";
//        String outputFolder = parentFolder + "/classbenchpartition/vmstart/imcsplit3/1";
//        String statsOutputFolder = parentFolder + "/classbenchpartition/vmstartstat/imcsplit3/1";
//        String topologyFile = parentFolder + "/topologylm/memory/tree_4096_1024.txt";
//        String ruleSize = "131072";
//        String paramSets = "2";// "1,2,3,4,5";

//        String inputRulesFolder = parentFolder + "/randomrules";
//        String inputFlowsFolder = parentFolder + "/randomflows";
//        String outputFolder = parentFolder + "/randompartition/vmstart/";
//        String statsOutputFolder = parentFolder + "/randompartition/vmstartstat/";
//        String topologyFile = parentFolder + "/topologylm/memory/tree_4096_1024.txt";
//        String ruleSize = "204800";
//        String paramSets = "";

        String inputRulesFolder = parentFolder + "/classbenchrules/converted";
        String inputFlowsFolder = parentFolder + "/classbenchflows/imc/nolocal_1/1";
        String outputFolder = parentFolder + "/classbenchpartition/vmstart/imc/nolocal_1/1";
        String statsOutputFolder = parentFolder + "/classbenchpartition/vmstartstas/imc/nolocal_1/1";
        String topologyFile = parentFolder + "/topologylm/memory/tree_0_4096.txt";
        String ruleSize = "131072";
        String paramSets = "2";// "1,2,3,4,5";

        ////////////////////////////////////////// RUN
        int[] ipAssigners = new int[]{IPAssigner.MACHINE_LEVEL_AGGREGATE, IPAssigner.DATACENTER_LEVEL_AGGREGATE};
        for (String paramSet : paramSets.split(",")) {
            for (int i = 0; i < ipAssigners.length; i++) {
                run(inputRulesFolder, inputFlowsFolder + "/" + ipAssigners[i] + "/flows", outputFolder + "/" + ipAssigners[i],
                        topologyFile, statsOutputFolder + "/" + ipAssigners[i],
                        ruleSize, paramSet, inputFlowsFolder + "/" + ipAssigners[i] + "/vms");
            }
        }

    }

    private static void run(String inputRulesFolder, String inputFlowsFolder, String outputFolder
            , String topologyFile, String statsOutputFolder, String ruleSize, String paramSet, String inputVmsFolder) throws IOException {
        new File(outputFolder).mkdirs();
        new File(statsOutputFolder).mkdirs();
        List<File> ruleFiles = Arrays.asList(new File(inputRulesFolder).listFiles());
        Collections.sort(ruleFiles);
        Map<String, Object> parameters = new HashMap<String, Object>();
        Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),
                topologyFile, parameters, new ArrayList<Topology>()).get(0);

        RuleFactory ruleFactory = new RuleFactory(
                new FileFactory.EndOfFileCondition());
        FlowFactory flowFactory = new FlowFactory(new FileFactory.EndOfFileCondition(), topology);
        Map<String, Object> parameters2 = new LinkedMap<String, Object>();
        for (File ruleFile : ruleFiles) {

            Util.loadParameters(ruleFactory, ruleFile.getPath(), parameters2);
            if ((ruleSize.isEmpty() || parameters2.get("rule.num").toString().equals(ruleSize)) && (
                    paramSet.isEmpty() || parameters2.get("rule.paramSet").toString().equals(paramSet))) {
                System.out.println(ruleFile);

                List<File> flowFiles = Arrays.asList(new File(inputFlowsFolder).listFiles());
                File flowFound = null;
                File vmsFound = null;
                for (File flowFile : flowFiles) {
                    if (Util.fromEqualRuleSet(ruleFile, flowFile, ruleFactory, flowFactory)) {
                        flowFound = flowFile;
                        break;
                    }
                }
                if (flowFound != null) {
                    if (inputVmsFolder != null && new File(inputVmsFolder).exists()) {
                        List<File> vmsFiles = Arrays.asList(new File(inputVmsFolder).listFiles());
                        VMAssignmentFactory vmAssignmentFactory = new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology);
                        for (File vmFile : vmsFiles) {
                            if (Util.fromEqualRuleSet(vmFile, flowFound, vmAssignmentFactory, flowFactory)) {
                                vmsFound = vmFile;
                                break;
                            }
                        }
                    }
                    run(ruleFile, outputFolder, parameters, topology, flowFound, statsOutputFolder, new UnifiedPartitionWriter(), vmsFound);
                    Runtime.getRuntime().gc();
                }
            }
        }
    }

    public static int run(File ruleFile, String outputFolder, Map<String, Object> parameters, Topology topology, File flowFile,
                          String statsOutputFolder, UnifiedPartitionWriter unifiedPartitionWriter,
                          File vmAssignmentFile) throws IOException {
        System.out.println("start " + flowFile);
        Collection<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()),
                ruleFile.getPath(), parameters, new LinkedList<Rule>());
        List<Switch> edges = topology.findEdges();
        Map<Long, Switch> vmSource;
        int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        if (vmAssignmentFile == null || !vmAssignmentFile.exists()) {
            vmSource = new HashMap<Long, Switch>();
            Collection<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology),
                    flowFile.getPath(), parameters, new LinkedList<Flow>());
            for (Flow flow : flows) {
                Long srcIP = flow.getProperty(srcIPIndex);
                vmSource.put(srcIP, flow.getSource());
            }
            flows.clear();
            flows = null;
        } else {
            vmSource = Util.loadFile(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology), vmAssignmentFile.getPath(),
                    parameters, new ArrayList<Map<Long, Switch>>()).get(0);
        }

        int numberOfVms = vmSource.size();
        System.out.println(flowFile + " to " + numberOfVms + " partitions");
        parameters.put("partition.num", numberOfVms);
        SourceVMPartition alg = new SourceVMPartition();
        parameters.put("partition.alg", alg);
        //MatrixRuleSet.setRuleSet((List<Rule>)rules);
        Collection<Partition> partitions = alg.partition(rules, vmSource.keySet(), new LinkedList<Rule>());//linked list is faster
//        assignToSources(topology, partitions, vmSource);
        writeStats(flowFile, statsOutputFolder, edges, vmSource, srcIPIndex, numberOfVms, partitions);

        final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder + "/" + flowFile.getName())));
        int nextRuleID = unifiedPartitionWriter.write(partitions, writer, parameters);
        writer.close();
        return nextRuleID;
    }

    /*private static void assignToSources(Topology topology, Collection<Partition> partitions, Map<Long, Switch> vmSource2) {
        //topology.createHelpers();//need to init helpers which need classified flows
        topology.initHelpers(new CollectionPool<Set<Rule>>(new HashSet<Rule>()),new CollectionPool<>() );
        TreeMap<Long, Switch> vmSource = new TreeMap<>(vmSource2);
        int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        for (Partition partition : partitions) {
            RangeDimensionRange srcIP = partition.getProperty(srcIPIndex);
            NavigableMap<Long, Switch> sources = vmSource.subMap(srcIP.getStart(), true, srcIP.getEnd(), true);

            try {
                for (Switch source : sources.values()) {
                    topology.getHelper(source).isAddFeasible(source, partition, sources.values(), false, true);
                }
            } catch (Switch.InfeasibleStateException e) {
                e.printStackTrace();
            }
        }
    }*/

    private static void writeStats(File flowFile, String statsOutputFolder, List<Switch> edges, Map<Long, Switch> vmSource,
                                   int srcIPIndex, int numberOfVms, Collection<Partition> partitions) throws FileNotFoundException {
        PrintWriter partitionSizeWriter = new PrintWriter(statsOutputFolder + "/p_" + flowFile.getName());
        Map<Switch, Set<Rule>> rules = new HashMap<>();
        for (Switch edge : edges) {
            rules.put(edge, new HashSet<Rule>());
        }
        boolean first = true;
        for (Partition partition : partitions) {
            partitionSizeWriter.print((first ? "" : ",") + partition.getSize());
            Switch src = vmSource.get(partition.getProperty(srcIPIndex).getStart());
            try {
                rules.get(src).addAll(partition.getRules());
            } catch (Exception e) {
                e.printStackTrace();
            }
            first = false;
        }
        partitionSizeWriter.close();
        PrintWriter machineRuleSizeWriter = new PrintWriter(statsOutputFolder + "/m_" + numberOfVms + "_" + flowFile.getName());
        first = true;
        for (Switch edge : edges) {
            machineRuleSizeWriter.print((first ? "" : ",") + edge.getUsedAbsoluteResources(edge.getState()));
            first = false;
        }
        machineRuleSizeWriter.close();
    }
}
