package edu.usc.enl.cacheflow.scripts.stats;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.partition.transform.PartitionTransformer;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePostPlaceScriptCluster2;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 11/30/12
 * Time: 9:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class LoadDistributionStats {
    public static void main(String[] args) throws IOException, UnalignedRangeException, NoAssignmentFoundException {

        String partitionFile;
        String outputStatsFile;
        HashMap<String, Object> parameters;
        Topology topology;
        List<Flow> flows;
        UnifiedPartitionFactory partitionFactory;
        List<Partition> partitions;
        Map<Switch, Collection<Partition>> sourcePartitions;

        Util.setRandom(Integer.parseInt(args[0]));
        Util.threadNum = Integer.parseInt(args[1]);
        String topologyFile = args[2];
        partitionFile = args[3];
        String flowFile = args[4];
        File vmsFile = new File(args[5]);
        outputStatsFile = args[6];

        //load topology
        parameters = new HashMap<>();
        topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.createDefaultAggregator(), new HashSet<Rule>()),
                topologyFile, parameters, new LinkedList<Topology>()).get(0);

        //load flows
        flows = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                topology), flowFile, parameters, new LinkedList<Flow>(), "flow\\..*");

        //load vm assignment
        Map<Long, Switch> vmSource = Util.loadFileFilterParam(new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology), vmsFile.getPath(),
                parameters, new ArrayList<Map<Long, Switch>>(), "flow\\..*").get(0);
        //load partitions
        partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), false, new HashSet<Rule>());
        partitions = Util.loadFileFilterParam(partitionFactory,
                partitionFile, parameters, new LinkedList<Partition>(), "(rule\\..*|partition\\..*)");

        //create sourcepartitions
        sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(partitions, vmSource);

        //clear feasibility checker
        Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, topology, sourcePartitions);

        //init topologies
        {
            final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows = new ThreadTwoLevelTrafficProcessor(
                    new OVSClassifier(),
                    new OVSClassifier(), Util.threadNum).classify(flows, partitions);

            topology.createHelpers(partitions, forwardingRules, classifiedFlows);
            topology.initHelpers(new CollectionPool<Set<Rule>>(new HashSet<Rule>()), new CollectionPool<Set<Long>>(new HashSet<Long>()));
        }

        File outputStatsFileFile = new File(outputStatsFile);
        LoadDistributionStats.serverLoadDistribution(outputStatsFileFile.getParent(), topology, partitions, sourcePartitions, outputStatsFileFile.getName());

    }

    public static double serverLoadDistribution(String partitionsOutputFolder, Topology simTopology, List<Partition> partitions,
                                                Map<Switch, Collection<Partition>> sourcePartitions, String partitionFileName)
            throws NoAssignmentFoundException, FileNotFoundException {
        double max = 0;
        Map<Switch, Double> load = PartitionTransformer.getVMStartAssignmentLoad(partitions, sourcePartitions, simTopology, null);
        PrintWriter pw = new PrintWriter(partitionsOutputFolder + "/" + partitionFileName);
        boolean first = true;
        for (Double l : load.values()) {
            pw.print((first ? "" : ",") + String.format("%.0f", l));
            first = false;
            max = max < l ? l : max;
        }
        pw.close();
        return max;
    }
}
