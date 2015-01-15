package edu.usc.enl.cacheflow.scripts.preliminaries.cluster.hierarchical;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/16/12
 * Time: 10:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateClusteringMatrix {
    public static void main(String[] args) throws IOException {
        //String partitionFile = "input\\nsdi\\partitiontenant\\vmstart\\-1\\20480_0_64_8_16_0.5_-1_1.0.txt";
        //String matrixFile = "input\\nsdi\\partitiontenant\\vmstart\\-1\\m_20480_0_64_8_16_0.5_-1_1.0.txt";
        // "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-1\\20480_classbench_131072_1.txt";
        // String matrixFile = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-1\\m_20480_classbench_131072_1.txt";
        String partitionFile = "input\\nsdi\\classbenchpartition\\vmstart\\imc\\20480_classbench_131072_2.txt";
        String matrixFile = "input\\nsdi\\classbenchpartition\\vmstart\\imc\\m_20480_classbench_131072_2.txt";
        final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), false, new HashSet<Rule>());
        saveInputMatrix(partitionFile, matrixFile, partitionFactory);
    }

    /**
     * THE INPUT MUST HAVE SORTED PARTITIONS BASED ON THEIR SRCIP DIMENSION
     *
     * @param partitionFile
     * @param matrixFile
     * @param partitionFactory
     * @throws IOException
     */
    public static void saveInputMatrix(String partitionFile, String matrixFile, UnifiedPartitionFactory partitionFactory) throws IOException {
        final List<Partition> partitions;
        Map<String, Object> parameters = new HashMap<String, Object>();
        {
            partitions = Util.loadFile(partitionFactory, partitionFile, parameters,new LinkedList<Partition>());
//            final List<Cluster> clusters = Util.loadFile(new ClusterFactory(new FileFactory.EndOfFileCondition(), partitions),
//                    //        "input\\nsdi\\clustertenant\\0\\0.txt");
//                    "input\\nsdismall\\cluster_200_1\\-1\\0.txt");

            System.out.println("loaded");
            /*{
                THE INPUT IS SORTED


                final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
                Comparator<Partition> partitionSrcIPStartComparator = new Comparator<Partition>() {
                    public int compare(Partition o1, Partition o2) {
                        return Long.compare(o1.getProperty(srcIPIndex).getStart(), o2.getProperty(srcIPIndex).getStart());
                    }
                };

                Collections.sort(partitions, partitionSrcIPStartComparator);
                int i = 1;
                for (Partition partition : partitions) {
                    partition.setId(i++);
                }
            }*/
        }
        /*System.out.println(partitionFactory.getRulesSize());
        Set<Rule> common = new HashSet<Rule>();
        for (Partition partition : partitions) {
            if (common.size() == 0) {
                common = new HashSet<Rule>(partition.getRules());
            } else {
                common.retainAll(partition.getRules());
            }
        }
        System.out.println(common.size());
        System.exit(0);*/


        ObjectOutputStream stream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(matrixFile)));
        stream.writeObject(parameters);
        stream.writeInt(partitions.size());
        stream.writeInt(partitionFactory.getRulesSize());
        int longNumbers = MatrixRuleSet.getLongNums(partitionFactory.getRulesSize());
        long[] partitionRuleMatrix = new long[longNumbers];
        for (Partition partition : partitions) {
            if (partition.getId() % 1000 == 0) {
                System.out.println(partition.getId());
            }
            stream.reset();
            MatrixRuleSet.convertToMatrix(partitionRuleMatrix, partition.getRules());

            stream.writeObject(partitionRuleMatrix);
        }
        stream.close();
    }

}
