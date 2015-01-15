package edu.usc.enl.cacheflow.scripts.twopolicy;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.partition.UnifiedPartitionWriter;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 9/17/12
 * Time: 9:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class VMStartTwoPolicyMergePartitions {
    public static void main(String[] args) {
        Map<String, Object> parameters = new HashMap<>();
        File partitionFile1 = new File(args[0]);
        File partitionFile2 = new File(args[1]);
        String partitionFileOut = args[2];

        new File(partitionFileOut).getParentFile().mkdirs();

        try {
            //load partitions1
            final UnifiedPartitionFactory partitionFactory1 = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
            List<Partition> partitions1 = Util.loadFileFilterParam(partitionFactory1, partitionFile1.getPath(), parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");

            //load partitions2
            final UnifiedPartitionFactory partitionFactory2 = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
            List<Partition> partitions2 = Util.loadFileFilterParam(partitionFactory2, partitionFile2.getPath(), parameters, new LinkedList<Partition>(), "(partition|rule)\\..*");

            //set id of 2 as rules in 1 (-1) +their current id, no need to be after each other, unifiedwriter take care of it
            int bias = partitionFactory1.getRulesSize() + 100;
            for (Rule rule : partitionFactory2.getRules()) {
                rule.setId(rule.getId() + bias);
            }
            partitions2.addAll(partitions1);

            //save partitions 1 without merging ids
            UnifiedPartitionWriter unifiedPartitionWriter2 = new UnifiedPartitionWriter();
            PrintWriter writer2 = new PrintWriter(partitionFileOut);
            unifiedPartitionWriter2.write(partitions2, writer2, parameters);
            writer2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
