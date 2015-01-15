package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/27/11
 * Time: 8:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class VMIPRangeClassBenchFlowGenerator2 extends IPRangeClassBenchFlowGenerator {
    public float paretoA = 1;
    public float paretoB = 0.1f;
    public int blockPerSource = 50;
    public int vmPerSource = 50;

    public VMIPRangeClassBenchFlowGenerator2(int blockPerSource, int vmPerSource) {
        this.blockPerSource = blockPerSource;
        this.vmPerSource = vmPerSource;
    }

    public VMIPRangeClassBenchFlowGenerator2(float paretoA, float paretoB, int blockPerSource, int vmPerSource) {
        this.paretoA = paretoA;
        this.paretoB = paretoB;
        this.blockPerSource = blockPerSource;
        this.vmPerSource = vmPerSource;
    }

    public List<Flow> generate(Random random, List<Switch> sources, List<Switch> destinations, List<String> classbenchProperties,
                               DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution) {

        List<Flow> flows = new ArrayList<Flow>(classbenchProperties.size());
        {
            //load flows
            loadFlows(random, sources, classbenchProperties, flowDistribution, flows);
        }


        int SRCIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);

        //need a data structure to find the flows of an IP fast
        Map<Long, Set<Flow>> srcIPFlows = new HashMap<Long, Set<Flow>>();
        //find unique sorted list of source IPs
        HashSet<Long> ips = new HashSet<Long>();
        for (Flow flow : flows) {
            final Long ip = flow.getProperty(SRCIPIndex);
            ips.add(ip);
            Set<Flow> thisIPFlows = srcIPFlows.get(ip);
            if (thisIPFlows == null) {
                thisIPFlows = new HashSet<Flow>();
                srcIPFlows.put(ip, thisIPFlows);
            }
            thisIPFlows.add(flow);
        }

        //sort
        List<Long> ipsSorted = new ArrayList<Long>(ips);
        Collections.sort(ipsSorted);

        final int numberOfBlocks = sources.size() * blockPerSource;
        final int IPperBlock = (int) Math.floor(1.0 * ipsSorted.size() / numberOfBlocks);
        final int vmsperBlock = vmPerSource / blockPerSource;
        if (IPperBlock < vmsperBlock) {
            Util.logger.warning("Fewer number of Unique IPs in the input flows than the number of VMs per blocks");
        }


        //assign Flows to blocks
        List<List<Flow>> blocks = new ArrayList<List<Flow>>(numberOfBlocks);

        for (int i1 = 0; i1 < numberOfBlocks; i1++) {
            //find IPs that are in the block
            List<Long> ipsInBlock = ipsSorted.subList(IPperBlock * i1, Math.min(ipsSorted.size(), IPperBlock * (i1 + 1)));
            //only keep IPs as the number of VMs in this block
            Collections.shuffle(ipsInBlock,random);
            ipsInBlock = ipsInBlock.subList(0, vmsperBlock);
            //find flows for the ips in this block
            List<Flow> flowsOfBlock = new LinkedList<Flow>();
            for (Long ip : ipsInBlock) {
                flowsOfBlock.addAll(srcIPFlows.get(ip));
            }
            blocks.add(flowsOfBlock);
        }

        Collections.shuffle(blocks,random);

        List<Flow> outputFlows = new LinkedList<Flow>();

        //assign flows of blocks to sources (migrateFrom them)
        final Iterator<List<Flow>> iterator = blocks.iterator();
        for (Switch source : sources) {
            int flowsPerSource = flowDistribution.getRandomFlowNum(random.nextDouble());
            List<Flow> flowsOfSource = new ArrayList<Flow>(flowsPerSource);
            for (int i = 0; i < blockPerSource; i++) {
                flowsOfSource.addAll(iterator.next());
            }

            destinationSelector.setSource(source, sources);
            //select a flow randomly migrateFrom it based on pareto distribution
            Collections.shuffle(flowsOfSource);
            int flowsNum = 0;
            Iterator<Flow> sourceFlowIterator = flowsOfSource.iterator();
            while (flowsNum < flowsPerSource) {
                if (!sourceFlowIterator.hasNext()){
                    //do it again
                    Collections.shuffle(flowsOfSource);
                    sourceFlowIterator = flowsOfSource.iterator();
                }
                Flow next = sourceFlowIterator.next();
                int replication = getPareto(random, paretoA, paretoB);
                flowsNum+=replication;
                if (flowsNum>flowsPerSource){
                    replication-=flowsNum-flowsPerSource;
                }
                next.setSource(source);
                for (int i = 0; i < replication ; i++) {
                    next.setDestination(destinationSelector.getDestination(random, source, sources));
                    next.setTraffic(flowDistribution.getRandomFlowSize(random.nextDouble()));
                    outputFlows.add(next);
                    next = next.duplicate();
                }
            }

        }
        return outputFlows;
    }

    // from classbench implementation
    public static int getPareto(Random random, float a, float b) {
        if (b == 0) return 1;

        // Random number
        double p;
        // Select random number
        p = random.nextDouble();

        double x = (double) b / Math.pow((double) (1 - p), (double) (1 / (double) a));

        return (int) Math.ceil(x);
    }
}
