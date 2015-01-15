package edu.usc.enl.cacheflow.processor.partition;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/13/12
 * Time: 2:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class BinaryPartitionWriter {
    //IF YOU MAKE COPY OF RULES IN EACH PARTITION CONSOLIDATE IT BEFORE USING THIS METHOD
    public void write(Collection<Partition> partitions, ObjectOutputStream writer,Map<String,Object>parameters) throws IOException {
        //write rules
        //Consolidate ids
        Set<Rule> idRules = new HashSet<Rule>(partitions.size());
        for (Partition partition : partitions) {
            for (Rule rule : partition.getRules()) {
                idRules.add(rule);
            }
        }
        int id=1;
        for (Rule idRule : idRules) {
            idRule.setId(id++);
        }
        id=1;
        for (Partition partition : partitions) {
            partition.setId(id++);
        }
        writer.writeObject(parameters);
        writer.writeObject(partitions);
    }
}
