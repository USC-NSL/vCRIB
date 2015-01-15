package edu.usc.enl.cacheflow.scripts.twopolicy;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.SrcVMFlowGenerator;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 9/18/12
 * Time: 5:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class MergeFlowsAndVMs {
    public static void main(String[] args) throws IOException {
        String topologyFile = args[0];
        String vmFile1 = args[1];
        String vmFile2 = args[2];
        String vmOutputFile = args[3];
        String flowFile1 = args[4];
        String flowFile2 = args[5];
        String flowOutputFile = args[6];


        Map<String, Object> parameters = new HashMap<>();
        Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),
                topologyFile, parameters, new LinkedList<Topology>()).get(0);
        {
            Map<Long, Switch> vmSource1 = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology), vmFile1,
                    parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
            Map<Long, Switch> vmSource2 = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology), vmFile2,
                    parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
            vmSource2.putAll(vmSource1);


            Map<Switch, List<Long>> switchIPMap = new HashMap<>();
            for (Map.Entry<Long, Switch> entry : vmSource2.entrySet()) {
                Switch aSwitch = entry.getValue();
                List<Long> ips = switchIPMap.get(aSwitch);
                if (ips == null) {
                    ips = new ArrayList<>();
                    switchIPMap.put(aSwitch, ips);
                }
                ips.add(entry.getKey());
            }
            //save to file
            File file = new File(vmOutputFile);
            file.getParentFile().mkdirs();

            SrcVMFlowGenerator.writeVMAssignment(vmOutputFile, switchIPMap, parameters);
        }
        //merge flows
        {
            List<Flow> flows1 = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                    topology), flowFile1, parameters, new LinkedList<Flow>(), "flow\\..*");
            List<Flow> flows2 = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                    topology), flowFile2, parameters, new LinkedList<Flow>(), "flow\\..*");
            flows2.addAll(flows1);
            File file = new File(flowOutputFile);
            file.getParentFile().mkdirs();
            WriterSerializableUtil.writeFile(flows2, file, false, parameters);
        }

    }
}
