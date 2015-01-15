package edu.usc.enl.cacheflow.scripts.preliminaries.flow;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased.SrcVMFlowGenerator;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.IPAssigner;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.ServerAggregateIPAssigner;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 11/4/12
 * Time: 6:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class ConvertVMAssignmentScript {
    public static void main(String[] args) throws IOException {
        Util.setRandom(Integer.parseInt(args[0]));
        String topologyFile = args[1];
        String vmAssignmentFile = args[2];
        String vmAssignmentFileName = args[3];


        ServerAggregateIPAssigner ipAssigner = new ServerAggregateIPAssigner(1);

        //load vms list
        Map<String, Object> parameters = new HashMap<>();
        Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.createDefaultAggregator(), new HashSet<Rule>()),
                topologyFile, parameters, new LinkedList<Topology>()).get(0);
        Map<Long, Switch> vmAssignment = Util.loadFile(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology), vmAssignmentFile,
                parameters, new ArrayList<Map<Long, Switch>>()).get(0);
        //convert
        parameters.put("flow.ipAssigner", ipAssigner);
        parameters.put("flow.ipAssigner.blockPerMachine", 1);

        Map<Switch, List<Long>> input = new HashMap<>();
        for (Map.Entry<Long, Switch> entry : vmAssignment.entrySet()) {
            List<Long> ips = input.get(entry.getValue());
            if (ips == null) {
                ips = new ArrayList<>();
                input.put(entry.getValue(), ips);
            }
            ips.add(entry.getKey());
        }

        Map<Switch, List<Long>> output = ipAssigner.convert(Util.random, input, topology);
        //save
        new File(vmAssignmentFileName).getParentFile().mkdirs();
        SrcVMFlowGenerator.writeVMAssignment(vmAssignmentFileName, output, parameters);
    }

}
