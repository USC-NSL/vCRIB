package edu.usc.enl.cacheflow.scripts.twopolicy;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.partition.UnifiedPartitionWriter;
import edu.usc.enl.cacheflow.scripts.preliminaries.partition.VMStartPartitions;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 9/13/12
 * Time: 2:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class VMStartTwoPolicyPartitions {
    public static void main(String[] args) {
        String parentFolder = "input\\nsdi";
        String ruleFileName1 = parentFolder + "\\classbenchrules\\split\\classbench_131072_2_1.txt";
        String ruleFileName2 = parentFolder + "\\classbenchrules\\split\\classbench_131072_2_2.txt";
        String topologyFile = parentFolder + "/topologylm/memory/tree_4096_1024.txt";
        String outputFolder1 = parentFolder + "\\classbenchpartition\\vmstart\\imcsplit\\1";
        String outputFolder2 = parentFolder + "\\classbenchpartition\\vmstart\\imcsplit\\2";
        File flowFile = new File(parentFolder + "\\classbenchflows\\imc\\-1\\flows\\classbench_131072_2.txt");
        File vmsFile = new File(parentFolder + "\\classbenchflows\\imc\\-1\\vms\\classbench_131072_2.txt");
        String statsOutputFolder1=outputFolder1+"/stat";
        String statsOutputFolder2=outputFolder2+"/stat";

        new File(outputFolder1).mkdirs();
        new File(outputFolder2).mkdirs();
        new File(statsOutputFolder1).mkdirs();
        new File(statsOutputFolder2).mkdirs();

        try {
            //load rules
            HashMap<String, Object> parameters = new HashMap<>();

            //load topology
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),
                    topologyFile, parameters, new ArrayList<Topology>()).get(0);

            //partition rule1
            int nextRuleID = VMStartPartitions.run(new File(ruleFileName1), outputFolder1, parameters,
                    topology, flowFile, statsOutputFolder1, new UnifiedPartitionWriter(), vmsFile);

            //save rule1 partitions

            //partition rule2 with startruleid
            VMStartPartitions.run(new File(ruleFileName2), outputFolder2, parameters, topology, flowFile,
                    statsOutputFolder2, new UnifiedPartitionWriter(nextRuleID),vmsFile);
            //save rule2 partitions
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
