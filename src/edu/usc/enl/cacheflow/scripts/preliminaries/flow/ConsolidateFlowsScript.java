package edu.usc.enl.cacheflow.scripts.preliminaries.flow;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.flow.ConsolidateFlowsProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/26/12
 * Time: 4:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class ConsolidateFlowsScript {
    public static void main(String[] args) {
        final String topologyFile = "input/hotcloud/topology/tree_20000_60000.txt";
        final String flowFile = "input/hotcloud/flows/true_true_5_50/classbench_4096_1.txt_trace";
        final String outputFile = "input/hotcloud/flows/true_true_5_50/conclassbench_4096_1.txt_trace";
        try {
            //load flows
            Map<String, Object> parameters = new HashMap<String, Object>();
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                    Util.createDefaultAggregator(), new HashSet<Rule>()), topologyFile, parameters, new LinkedList<Topology>()).get(0);
            List<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology),
                    flowFile, parameters, new LinkedList<Flow>());
            final List<Flow> newFlows = new ConsolidateFlowsProcessor().process(flows);
            WriterSerializableUtil.writeFile(newFlows, new File(outputFile), false, parameters);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
