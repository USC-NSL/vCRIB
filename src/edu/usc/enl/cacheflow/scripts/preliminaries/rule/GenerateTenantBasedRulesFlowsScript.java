package edu.usc.enl.cacheflow.scripts.preliminaries.rule;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.IPAssigner;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.RackAggregateIPAssigner;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.ServerAggregateIPAssigner;
import edu.usc.enl.cacheflow.processor.rule.generator.OnlyTenantAccessControlRuleFlowGenerator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/15/12
 * Time: 5:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateTenantBasedRulesFlowsScript {
    public static void main(String[] args) throws IOException {
        String parentFolder = "input/nsdi";
        String topologyFile = parentFolder + "/topologylm/memory/tree_4096_1024.txt";
        int tenantNum = 64;
        int minIPRangePerTenant = 1;
        int maxIPRangePerTenant = 1;
        int minIPRangeSize = 8;
        int maxIPRangeSize = 16;
        double interTenantRuleAcceptProb = 0.5;
        int vmPerSource = 20;
        int[] blockPerSources = new int[]{IPAssigner.DATACENTER_LEVEL_AGGREGATE,
                IPAssigner.MACHINE_LEVEL_AGGREGATE,
                IPAssigner.RACK_LEVEL_AGGREGATE};
        int flowPerMachine = 1000;
        double[] intraTenantTrafficProbs = new double[]{0.25, 1};
        int randomSeedIndex = 0;

        String flowDescriptionFile = "input/imcflowspecs.txt";

        new File(parentFolder + "/tenantrules").mkdirs();
        new File(parentFolder + "/tenantflows").mkdirs();

        Util.setRandom(randomSeedIndex);
        Map<String, Object> parametersFlow = new HashMap<String, Object>();
        final OnlyTenantAccessControlRuleFlowGenerator generator = new OnlyTenantAccessControlRuleFlowGenerator();
        List<Rule> rules = generator.generateRules(Util.random, minIPRangePerTenant, maxIPRangePerTenant, minIPRangeSize, maxIPRangeSize, tenantNum, interTenantRuleAcceptProb);
        Map<String, Object> parametersRule = new HashMap<String, Object>();

        String ruleOutputFile = parentFolder + "/tenantrules/" + randomSeedIndex + "_" + tenantNum + "_" + minIPRangeSize + "_" + maxIPRangeSize
                + "_" + interTenantRuleAcceptProb + ".txt";
        parametersRule.put("rule.generator", generator);
        parametersRule.put("rule." + generator + ".tenantsNum", tenantNum);
        parametersRule.put("rule." + generator + ".interTenantRuleAcceptProb", interTenantRuleAcceptProb);
        parametersRule.put("randomSeedIndex", randomSeedIndex);
        WriterSerializableUtil.writeFile(rules, new File(ruleOutputFile), false, parametersRule);
        CustomRandomFlowDistribution flowDistribution = new CustomRandomFlowDistribution(Util.loadFile(new File(flowDescriptionFile)));
        final Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()), topologyFile, parametersFlow,new LinkedList<Topology>()).get(0);
        for (int blockPerSource : blockPerSources) {
            IPAssigner ipAssigner = null;
            if (blockPerSource == IPAssigner.DATACENTER_LEVEL_AGGREGATE) {
                ipAssigner = new ServerAggregateIPAssigner(vmPerSource);
                parametersFlow.put("flow.ipAssigner.blockPerMachine", vmPerSource);
            } else if (blockPerSource == IPAssigner.MACHINE_LEVEL_AGGREGATE) {
                ipAssigner = new ServerAggregateIPAssigner(1);
                parametersFlow.put("flow.ipAssigner.blockPerMachine", 1);
            } else if (blockPerSource == IPAssigner.RACK_LEVEL_AGGREGATE) {
                ipAssigner = new RackAggregateIPAssigner(1);
                parametersFlow.put("flow.ipAssigner.blockPerMachine", 1);
            }
            for (double intraTenantTrafficProb : intraTenantTrafficProbs) {
                String flowOutputFile = parentFolder + "/tenantflows/" + blockPerSource + "/" + randomSeedIndex + "_" + tenantNum + "_" + minIPRangeSize + "_" + maxIPRangeSize
                        + "_" + interTenantRuleAcceptProb + "_" + blockPerSource + "_" + intraTenantTrafficProb + ".txt";
                new File(ruleOutputFile).getParentFile().mkdirs();
                new File(flowOutputFile).getParentFile().mkdirs();
                Util.setRandom(randomSeedIndex);
                try {

                    parametersFlow.put("flow." + generator + ".ipAssigner", ipAssigner);
                    parametersFlow.put("flow." + generator + ".intraTenantTrafficProb", intraTenantTrafficProb);
                    parametersFlow.putAll(parametersRule);
                    List<Flow> flows = generator.generateFlows(Util.random, flowDistribution, flowPerMachine, intraTenantTrafficProb, ipAssigner, topology);
                    WriterSerializableUtil.writeFile(flows, new File(flowOutputFile), false, parametersFlow);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
