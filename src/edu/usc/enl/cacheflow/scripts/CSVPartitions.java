package edu.usc.enl.cacheflow.scripts;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/15/12
 * Time: 12:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class CSVPartitions {
    public static void main(String[] args) throws IOException {
//        String inputFile = "input\\nsdismall\\partition2\\vmstart\\-1\\2559_classbench_8192_2.txt";
//        String outputFile = "input\\nsdismall\\partition2\\vmstart\\-1\\2559_classbench_8192_2.csv";

         String inputFile ="input\\nsdismall\\partitiontenant\\vmstart\\2560_0_64_8_16_0.5_-1_0.25.txt";
        String outputFile ="input\\nsdismall\\partitiontenant\\vmstart\\2560_0_64_8_16_0.5_-1_0.25.csv";

        PrintWriter writer = new PrintWriter(outputFile);

        UnifiedPartitionFactory factory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), false, new HashSet<Rule>());
        List<Partition> partitions = Util.loadFile(factory, inputFile,new HashMap<String, Object>(),new LinkedList<Partition>());
        {
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
        }
        for (Partition partition : partitions) {
            boolean[] rules = new boolean[factory.getRulesSize()];
            Arrays.fill(rules, false);
            for (Rule rule : partition.getRules()) {
                rules[rule.getId() - 1] = true;
            }
            boolean first = true;
            for (boolean rule : rules) {
                if (first) {
                    writer.write(rule ? "1" : "0");
                    first = false;
                } else {
                    writer.write(rule ? ",1" : ",0");
                }
            }
            writer.println();
        }
        writer.close();
    }
}
