package edu.usc.enl.cacheflow.scripts.preliminaries;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.topology.tree.FatTreeTopologyGenerator;
import edu.usc.enl.cacheflow.processor.topology.tree.TreeTemplate;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/1/12
 * Time: 9:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateTreeTopologies {
    public static void main(String[] args) {
        int[] controllerConfig = new int[]{100000};
        long[][] linksCapacity = new long[][]{{1l << 50, 1l << 50, 1l << 50}};
//        int[] switchCPUCapacities = new int[]{5, 10, 20};
//        int[] switchMemoryCapacities = new int[]{512*2, 2048*2};
//        int[] internalSwitchCapacities = new int[]{0, 2048*2, 3072*2};

        int[] switchCPUCapacities = new int[]{1<< 20};
        int[] switchMemoryCapacities = new int[]{ 1<<20};
        int[] internalSwitchCapacities = new int[]{ 1<<20};

        int degree = 8;
        String outputFolder = "input/nsdismall/topologyvmstart";
        {
            File outputFolderFile = new File(outputFolder + "/memory");
            outputFolderFile.mkdirs();
        }
        {
            File outputFolderFile = new File(outputFolder + "/cpu");
            outputFolderFile.mkdirs();
        }

        try {
            {
                Map<String, Object> parameters = new HashMap<String, Object>();
                parameters.put("topology.fatTreeDegree", degree);
                parameters.put("topology.edgeType", "OVS");
                for (int switchCPUCapacity : switchCPUCapacities) {
                    for (int internalSwitchCapacity : internalSwitchCapacities) {
                        parameters.put("topology.internalMemory", internalSwitchCapacity);
                        parameters.put("topology.cpuBudget", switchCPUCapacity);

                        StringBuilder sb = new StringBuilder();
                        sb.append("#nodes\n");
                        sb.append("Controller 10000 1000\n");
                        sb.append("Core Memory " + internalSwitchCapacity + "\n");
                        sb.append("Agg1 Memory " + internalSwitchCapacity + "\n");
                        sb.append("Agg2 Memory " + internalSwitchCapacity + "\n");
                        sb.append("Edge OVS " + switchCPUCapacity + "\n");
                        sb.append("#linkes\n");
                        sb.append("Core Agg1 " + linksCapacity[0][0] + "\n");
                        sb.append("Agg1 Agg2 " + linksCapacity[0][1] + "\n");
                        sb.append("Agg2 Edge " + linksCapacity[0][2] + "\n");

                        TreeTemplate template = new TreeTemplate(sb.toString());
                        FatTreeTopologyGenerator treeTopologyGenerator = new FatTreeTopologyGenerator(template, degree);
                        final Topology topology = treeTopologyGenerator.generate(Util.DEFAULT_AGGREGATOR, new HashSet<Rule>());
                        WriterSerializableUtil.writeFile(topology, new File(outputFolder + "/cpu/tree_" +
                                internalSwitchCapacity + "_" + switchCPUCapacity + ".txt"), false, parameters);
                    }
                }
            }
            {
                Map<String, Object> parameters = new HashMap<String, Object>();
                parameters.put("topology.fatTreeDegree", degree);
                parameters.put("topology.edgeType", "Memory");
                for (int switchMemoryCapacity : switchMemoryCapacities) {
                    for (int internalSwitchCapacity : internalSwitchCapacities) {
                        parameters.put("topology.internalMemory", internalSwitchCapacity);
                        parameters.put("topology.edgeMemory", switchMemoryCapacity);
                        StringBuilder sb = new StringBuilder();
                        sb.append("#nodes\n");
                        sb.append("Controller 10000 1000\n");
                        sb.append("Core Memory " + internalSwitchCapacity + "\n");
                        sb.append("Agg1 Memory " + internalSwitchCapacity + "\n");
                        sb.append("Agg2 Memory " + internalSwitchCapacity + "\n");
                        sb.append("Edge Memory " + switchMemoryCapacity + "\n");
                        sb.append("#linkes\n");
                        sb.append("Core Agg1 " + linksCapacity[0][0] + "\n");
                        sb.append("Agg1 Agg2 " + linksCapacity[0][1] + "\n");
                        sb.append("Agg2 Edge " + linksCapacity[0][2] + "\n");

                        TreeTemplate template = new TreeTemplate(sb.toString());
                        FatTreeTopologyGenerator treeTopologyGenerator = new FatTreeTopologyGenerator(template, degree);
                        final Topology topology = treeTopologyGenerator.generate(Util.DEFAULT_AGGREGATOR, new HashSet<Rule>());
                        WriterSerializableUtil.writeFile(topology, new File(outputFolder + "/memory/tree_" +
                                internalSwitchCapacity + "_" + switchMemoryCapacity + ".txt"), false, parameters);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
