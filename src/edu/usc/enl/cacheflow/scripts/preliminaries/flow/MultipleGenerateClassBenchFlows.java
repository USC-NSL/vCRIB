package edu.usc.enl.cacheflow.scripts.preliminaries.flow;

import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.NormalVMIPSelector;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.UniformRangeVMIPSelector;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.VMIPSelector;
import edu.usc.enl.cacheflow.util.Util;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/22/12
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class MultipleGenerateClassBenchFlows {
    public static void main(String[] args) throws IOException {
        String topologyFile;
        String flowDescriptionFile;
        String classbenchRulesFolder;
        String outputFolder;
        String[] paramSets;
        String[] blockPerMachines;
        String ruleSize;
        flowDescriptionFile = "input/imcflowspecs_nolocal.txt";
        int randomSeedIndex;

        randomSeedIndex = Integer.parseInt(args[0]);
        Util.threadNum = Integer.parseInt(args[1]);
        topologyFile = args[2];
        classbenchRulesFolder = args[3];
        ruleSize = args[4];
        paramSets = args[5].split(",");
        blockPerMachines = args[6].split(",");
        outputFolder = args[7];

//        VMIPSelector vmipSelector = new NormalVMIPSelector(1.0/6, 0.5);
        VMIPSelector vmipSelector = new UniformRangeVMIPSelector();

        for (String blockPerMachine : blockPerMachines) {
            for (String paramSet : paramSets) {
                GenerateFlowsClassbenchRules.run(topologyFile, flowDescriptionFile, classbenchRulesFolder, outputFolder + "/" + blockPerMachine,
                        paramSet,  Integer.parseInt(blockPerMachine), ruleSize, randomSeedIndex, vmipSelector);
            }
        }
    }
}
