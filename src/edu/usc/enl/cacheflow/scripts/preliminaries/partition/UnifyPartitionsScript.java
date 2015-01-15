package edu.usc.enl.cacheflow.scripts.preliminaries.partition;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.PartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.partition.UnifiedPartitionWriter;
import edu.usc.enl.cacheflow.processor.partition.UnifyPartitions;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/13/12
 * Time: 3:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class UnifyPartitionsScript {
    public static void main(String[] args) {
        String ruleSizes = "4096|8192|16384|32768";
        String inputFolder = "input/osdi/partition/half";
        String outputFolder = "input/osdi/partition/half2";

        try {
            File outputFolderFile = new File(outputFolder);
            outputFolderFile.mkdirs();

            File inputFolderFile = new File(inputFolder);
            for (File file : inputFolderFile.listFiles()) {
                if (file.getName().matches(".*(" + ruleSizes + ").*")) {
                    Map<String, Object> parameters = new HashMap<String, Object>();
                    final List<Partition> partitions = Util.loadFile(new PartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()),
                            file.getPath(), parameters,new LinkedList<Partition>());
                    final Collection<Partition> unifiedPartitions = new UnifyPartitions().unify(partitions);
                    final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder + "/" + file.getName())));
                    new UnifiedPartitionWriter().write(unifiedPartitions, writer, parameters);
                    writer.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
