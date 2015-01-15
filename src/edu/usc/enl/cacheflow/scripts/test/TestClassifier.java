package edu.usc.enl.cacheflow.scripts.test;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.PartitionFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.flow.classifier.LinearMatchTrafficProcessor;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.TwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/6/12
 * Time: 7:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestClassifier {
    public static void main(String[] args) {
        Util.logger.setLevel(Level.ALL);

        String ruleFile = "input/osdi/classbenchrules/converted/classbench_16384_1.txt";

        String topologyFile = "input/osdi/topology/20/tree_1000_100.txt";
        String flowFile = "input/osdi/flows/local_20_1/classbench_16384_2.txt";
        String partitionFile = "input/osdi/partitionequal/classbench/1024_classbench_16384_2.txt";

        //final Processor<?, List<Rule>> ruleProcessor = new StringToRuleProcessor(new OpenFileProcessor(new File(ruleFile)));
        Map<String, Object> parameters = new HashMap<String, Object>();
        try {
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),
                    topologyFile, parameters,new LinkedList<Topology>()).get(0);

            List<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology), flowFile, parameters,new LinkedList<Flow>());

            List<Partition> partitions = Util.loadFile(new PartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()), partitionFile, parameters,new LinkedList<Partition>());


            //new OVSClassifier(ruleProcessor,flowProcessor).run();
            long start = System.currentTimeMillis();
            final Map<Partition, Map<Rule, Collection<Flow>>> run = new TwoLevelTrafficProcessor(
                    new LinearMatchTrafficProcessor(),
                    new LinearMatchTrafficProcessor()).classify(flows, partitions);
            long now = System.currentTimeMillis();
            System.out.println("+++" + (now - start));
            final Map<Partition, Map<Rule, Collection<Flow>>> run2 = new TwoLevelTrafficProcessor(
                    new OVSClassifier(),
                    new OVSClassifier()).classify(flows, partitions);
            System.out.println("+++" + (System.currentTimeMillis() - now));
            /*for (Map.Entry<Partition, Map<Rule, List<Flow>>> entry : run.entrySet()) {
                final Map<Rule, List<Flow>> ruleListMap1 = entry.getValue();
                final Map<Rule, List<Flow>> ruleListMap2 = run2.get(entry.getKey());
                for (Map.Entry<Rule, List<Flow>> entry1 : ruleListMap1.entrySet()) {
                    final List<Flow> value2 = ruleListMap2.get(entry1.getKey());
                    if (value2 == null && entry1.getValue() != null && entry1.getValue().size() > 0) {
                        System.out.println();
                    }
                    if (value2 != null) {
                        int diff = value2.size() - entry1.getValue().size();
                        if (diff > 0) {
                            System.out.println();
                        }
                    }
                }
            }*/
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println();
    }
}
