package edu.usc.enl.cacheflow.scripts.comparealgorithms;

import edu.usc.enl.cacheflow.algorithms.feasibility.memory.compare.CompareFeasibleSolution;
import edu.usc.enl.cacheflow.algorithms.feasibility.memory.compare.GenerateCommonRuleTree;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.scripts.preliminaries.cluster.hierarchical.GenerateClusteringMatrix;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/30/12
 * Time: 2:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class MulitpleCompareFeasibleScript {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //String partitionFile = "input\\nsdismall\\partitionclassbench\\vmstart\\uniform\\-1\\2560_classbench_32768_2.txt";
//        String partitionFile = "input\\nsdi\\classbenchpartition\\vmstart\\imc\\20480_classbench_131072_2.txt";
       //String partitionFile = "input\\nsdi\\partitiontenant\\vmstart\\-1\\20480_0_64_8_16_0.5_-1_0.25.txt";
        String partitionFile="input\\nsdi\\randompartition\\vmstart\\-3\\4_204800.txt";

        File partitionFileFile = new File(partitionFile);
        String matrixFile = partitionFileFile.getParent() + "/m_" + partitionFileFile.getName();
        if (!new File(matrixFile).exists()) {
            GenerateClusteringMatrix.saveInputMatrix(partitionFile, matrixFile, new UnifiedPartitionFactory(
                    new FileFactory.EndOfFileCondition(), new HashSet<Rule>()));
        }
        String linkageFile = partitionFileFile.getParent() + "/lc_" + partitionFileFile.getName();
        if (!new File(linkageFile).exists()) {
            GenerateCommonRuleTree.loadAndRun(matrixFile, linkageFile);
        }
        String placementFolder = partitionFileFile.getParent() + "/" + (partitionFileFile.getName().replaceAll("\\..*$", ""));
        CompareFeasibleSolution.loadAndRun(matrixFile, linkageFile, placementFolder);

    }


}
