package edu.usc.enl.cacheflow.scripts.vcrib;

import edu.usc.enl.cacheflow.algorithms.placement.AssignerCluster;
import edu.usc.enl.cacheflow.algorithms.placement.clusterselection.ClusterSelection;
import edu.usc.enl.cacheflow.algorithms.placement.clusterselection.MaxRuleSizeClusterSelection;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectioncluster.SwitchSelectionCluster;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectioncluster.ThreadMinTrafficSameRuleSwitchSelectionCluster;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.OVSClassifier;
import edu.usc.enl.cacheflow.processor.flow.classifier.ThreadTwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.util.*;
import java.util.logging.Level;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/1/12
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class MultiplePlacementScriptCluster {

    public static void main(String[] args) throws Exception {
        Util.threadNum = Integer.parseInt(args[0]);
        int randomSeedIndex = Integer.parseInt(args[1]);
        String maxTopology = args[2];
        File topologyFolder = new File(args[3]);
        File flowFile = new File(args[4]);
        File partitionFile = new File(args[5]);
        String outputFolder = args[6];
        File clusterFile = new File(args[7]);

        Map<String, Object> parameters = new HashMap<String, Object>();
        //run(flowsFile, partitionFile, topologyFolder, parameters, maxTopology, randomSeedIndex, outputFolder);
        //laundry stuff
        Util.setRandom(randomSeedIndex);
        Util.logger.setLevel(Level.WARNING);
        {
            File outputFolderFile = new File(outputFolder);
            outputFolderFile.mkdirs();
        }

        //set parameters
        ClusterSelection partitionSelection = new MaxRuleSizeClusterSelection();

        //load data
        final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>());
        List<Partition> partitions = Util.loadFile(partitionFactory, partitionFile.getPath(), parameters, new LinkedList<Partition>());
        String maxTopologyPath = topologyFolder.getPath() + "/" + maxTopology;
        Topology simTopology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), maxTopologyPath, parameters, new LinkedList<Topology>()).get(0);
        List<Flow> flows = Util.loadFileFilterParam(new FlowFactory(new FileFactory.EndOfFileCondition(),
                simTopology), flowFile.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");
        final Map<Partition, Map<Rule, Collection<Flow>>> classifiedFlows = new ThreadTwoLevelTrafficProcessor(
                new OVSClassifier(),
                new OVSClassifier(), Util.threadNum).classify(flows, partitions);

        simTopology.setRuleFlowMap(classifiedFlows);

        List<Cluster> clusters = Util.loadFile(new ClusterFactory(new FileFactory.EndOfFileCondition(), partitions),
                clusterFile.getPath(), parameters, new LinkedList<Cluster>());
        final Map<Switch, Collection<Partition>> sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(classifiedFlows);
        Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, simTopology, sourcePartitions);

        SwitchSelectionCluster switchSelection = new ThreadMinTrafficSameRuleSwitchSelectionCluster(Util.threadNum, new CollectionPool<Set<Rule>>(new HashSet<Rule>()));
        AssignerCluster placer = new AssignerCluster(switchSelection, partitionSelection, 100, false, forwardingRules, clusters, sourcePartitions);
        parameters.put("placement.alg", placer);
        parameters.put("placement." + placer + ".partitionSelection", partitionSelection);
        parameters.put("placement." + placer + ".switchSelection", switchSelection);

        //run for max configuration
        MultiplePlacementScript.runForTopologies(maxTopology, topologyFolder, outputFolder, parameters, simTopology,
                flows,  placer, partitions);
    }

}
