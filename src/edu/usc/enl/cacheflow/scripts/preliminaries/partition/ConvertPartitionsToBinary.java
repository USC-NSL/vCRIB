package edu.usc.enl.cacheflow.scripts.preliminaries.partition;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.partition.BinaryPartitionWriter;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/24/12
 * Time: 12:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class ConvertPartitionsToBinary {
    public static void main(String[] args) throws IOException {
        String inputFile="";
        String outputFile="";
        //load partitions
        HashMap<String, Object> parameters = new HashMap<String, Object>();
        Collection<Partition> partitions = Util.loadFile(new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), false, new HashSet<Rule>()), inputFile, parameters,new LinkedList<Partition>());
        new BinaryPartitionWriter().write(partitions,new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile))), parameters);
    }
}
