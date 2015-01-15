package edu.usc.enl.cacheflow.scripts.preliminaries.flow;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.NormalVMIPSelector;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.SrcVMFlowGenerator;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.UniformRangeVMIPSelector;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.VMIPSelector;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.LocalizedDestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.IPAssigner;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.RackAggregateIPAssigner;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.ServerAggregateIPAssigner;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/26/12
 * Time: 6:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateFlowsClassbenchRules {
    public static void main(String[] args) throws IOException {
        String topologyFile;
        String flowDescriptionFile;
        String classbenchRulesFolder;
        String outputFolder;
        String paramSet;
        int blockPerMachine = 50;
        String ruleSize;
        flowDescriptionFile = "input/imcflowspecs.txt";
        int randomSeedIndex;

        randomSeedIndex = Integer.parseInt(args[0]);
        Util.threadNum = Integer.parseInt(args[1]);
        topologyFile = args[2];
        classbenchRulesFolder = args[3];
        ruleSize = args[4].startsWith("\\") ? args[4].replace("\\", "") : args[4];
        paramSet = args[5].startsWith("\\") ? args[5].replace("\\", "") : args[5];
        blockPerMachine = Integer.parseInt(args[6]);
        outputFolder = args[7];

        long middle = Util.SRC_IP_INFO.getDimensionRange().getSize() / 2;
        VMIPSelector vmipSelector = new UniformRangeVMIPSelector();

//        long middle = Util.SRC_IP_INFO.getDimensionRange().getSize() / 2;
//        VMIPSelector vmipSelector = new UniformRangeVMIPSelector(new RangeDimensionRange(0, middle-1, Util.SRC_IP_INFO));
        //NormalVMIPSelector(1.0/3, 0.5);


        run(topologyFile, flowDescriptionFile, classbenchRulesFolder, outputFolder, paramSet,
                blockPerMachine, ruleSize, randomSeedIndex, vmipSelector);
    }

    public static void run(String topologyFile, String flowDescriptionFile, String classbenchRulesFolder, String outputFolder,
                           String paramSet, int blockPerMachine, String ruleSize, int randomSeedIndex,
                           VMIPSelector vmipSelector) throws IOException {
        new File(outputFolder).mkdirs();


        Map<String, Object> parameters = new HashMap<String, Object>();
        IPAssigner ipAssigner = null;
        if (blockPerMachine == IPAssigner.DATACENTER_LEVEL_AGGREGATE) {
            ipAssigner = new ServerAggregateIPAssigner(ServerAggregateIPAssigner.NO_AGGREGATION);
            parameters.put("flow.ipAssigner.blockPerMachine", "perMachine");
        } else if (blockPerMachine == IPAssigner.MACHINE_LEVEL_AGGREGATE) {
            ipAssigner = new ServerAggregateIPAssigner(1);
            parameters.put("flow.ipAssigner.blockPerMachine", 1);
        } else if (blockPerMachine == IPAssigner.RACK_LEVEL_AGGREGATE) {
            ipAssigner = new RackAggregateIPAssigner(1);
            parameters.put("flow.ipAssigner.blockPerMachine", 1);
        }
        parameters.put("flow.ipAssigner", ipAssigner);
        parameters.put("flow.local", true);
        parameters.put("flow.edge", true);
        parameters.put("flow.vmIPSelector", 0);


        Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),
                topologyFile, parameters, new LinkedList<Topology>()).get(0);
        CustomRandomFlowDistribution flowDistribution = new CustomRandomFlowDistribution(Util.loadFile(new File(flowDescriptionFile)));
        LocalizedDestinationSelector localizedDestinationSelector = new LocalizedDestinationSelector(topology,
                flowDistribution.getLocalizedFlowDistribution());

        final List<File> files = Arrays.asList(new File(classbenchRulesFolder).listFiles());
        Collections.sort(files);
        RuleFactory ruleFactory = new RuleFactory(new FileFactory.EndOfFileCondition());
        Map<String, Object> parameters2 = new HashMap<String, Object>();
        for (File file : files) {
            Util.loadParameters(ruleFactory, file.getPath(), parameters2);
            if ((parameters2.get("rule.num") == null || parameters2.get("rule.num").toString().matches(ruleSize)) &&
                    (parameters2.get("rule.paramSet") == null || parameters2.get("rule.paramSet").toString().matches(paramSet))) {
                if (parameters2.containsKey("randomSeedIndex")) {
                    String randomSeedIndex1 = parameters2.get("randomSeedIndex").toString();
                    if (!randomSeedIndex1.equals("" + randomSeedIndex)) {
                        Util.logger.warning("randomSeedIndex provided is different from the input " + randomSeedIndex1 + " != " + randomSeedIndex + ". " +
                                "Will use rule file randomSeedIndex=" + randomSeedIndex);
                        //randomSeedIndex = Integer.parseInt(randomSeedIndex1);
                    }
                }
                Util.setRandom(randomSeedIndex);
                parameters.put("randomSeedIndex", randomSeedIndex);

                Collection<Rule> rules = Util.loadFile(ruleFactory, file.getPath(), parameters, new LinkedList<Rule>());//no need for a hashset

                String flowFileName = outputFolder + "/flows/" + file.getName();
                new File(flowFileName).getParentFile().mkdirs();
                System.out.println(flowFileName);
//                RuleIPRuleFlow flowGenerator = new RuleIPRuleFlow(vmPerMachine, null, ipAssigner, Util.threadNum);
//                parameters.put("flow.generator", flowGenerator);
//                writer.println(Statistics.getParameterLine(parameters));
//                flowGenerator.generate(rules, Util.random, topology.findEdges(), localizedDestinationSelector, flowDistribution, writer, topology);
                SrcVMFlowGenerator flowGenerator = new SrcVMFlowGenerator();
                parameters.put("flow.generator", flowGenerator);

                String vmsFilename = outputFolder + "/vms/" + file.getName();
                new File(vmsFilename).getParentFile().mkdirs();
                flowGenerator.generate(rules, Util.random, flowDistribution, topology, localizedDestinationSelector,
                        ipAssigner, flowFileName, false, vmipSelector, vmsFilename, parameters);
            }
        }
        System.out.println("finish");
    }
}
