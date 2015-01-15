package edu.usc.enl.cacheflow.processor.network;

import edu.usc.enl.cacheflow.algorithms.feasibility.general.FeasiblePlacer;
import edu.usc.enl.cacheflow.algorithms.migration.AbstractMigrator;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchhelper.SwitchHelper;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.OVSSwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/28/12
 * Time: 5:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class VMStartMigrateProcessor extends AbstractMigrator implements InformOnRestart {
    private final Random random;
    private final int threadNum;
    private final Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;
    private final int num;
    private Map<Long, MigrationStep> migrationSteps;
    private static final int tryPerVM = 200;

    public VMStartMigrateProcessor(Topology topology, Random random, int threadNum, Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap,
                                   Map<Partition, Long> minTraffics, Map<Switch, Collection<Partition>> sourcePartitions,
                                   Map<Partition, Map<Switch, Rule>> forwardingRules, int num) {
        super(topology, minTraffics, sourcePartitions, forwardingRules);
        this.random = random;
        this.threadNum = threadNum;
        this.ruleFlowMap = ruleFlowMap;
        this.num = num;
    }

    public Map<Partition, Switch> postPlace2(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter) {
        migrationSteps = new LinkedHashMap<>();
        final Map<Switch, Set<Partition>> rplacement = assignment.getRplacement();
        //vms and partitions are the same
        //sort vms in terms of traffic!
        final Map<Partition, Long> partitionsTraffic = new HashMap<>(ruleFlowMap.size());
        {
            Thread[] threads = new Thread[threadNum];
            final Iterator<Map.Entry<Partition, Map<Rule, Collection<Flow>>>> iterator = ruleFlowMap.entrySet().iterator();
            for (int i = 0; i < threadNum; i++) {
                threads[i] = new Thread() {
                    @Override
                    public void run() {

                        while (true) {
                            Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry;
                            synchronized (iterator) {
                                if (iterator.hasNext()) {
                                    entry = iterator.next();
                                } else {
                                    break;
                                }
                            }
                            long sum = 0;
                            for (Map.Entry<Rule, Collection<Flow>> entry1 : entry.getValue().entrySet()) {
                                for (Flow flow : entry1.getValue()) {
                                    sum += flow.getTraffic();
                                }
                            }
                            partitionsTraffic.put(entry.getKey(), sum);
                        }
                    }
                };
            }
            Util.runThreads(threads);
        }
        List<Partition> partitions = new ArrayList<>(ruleFlowMap.keySet());
        Collections.sort(partitions, new Comparator<Partition>() {
            @Override
            public int compare(Partition o1, Partition o2) {
                return -Long.compare(partitionsTraffic.get(o1), partitionsTraffic.get(o2));
            }
        });

        // select num random vms
        ArrayList<Switch> edges = new ArrayList<>(sourcePartitions.keySet());
        final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        int tryNum = 0;
        for (int i = 0; i < num; i++) {
            boolean successful = false;
            while (!successful) {
                //final Map.Entry<Switch, Collection<Partition>> src = getRandomEntry(sourcePartitions.entrySet());
                //final Partition partition = getRandomEntry(src.getValue());
                final Partition partition = partitions.get(tryNum++);
                final Switch srcSwitch = forwardingRules.get(partition).keySet().iterator().next();//src.getKey();
                Collections.sort(edges, new Comparator<Switch>() {
                    @Override
                    public int compare(Switch o1, Switch o2) {
                        return Double.compare(o2.getUsedResources(o2.getState()), o1.getUsedResources(o1.getState()));
                    }
                });
                Iterator<Switch> itr = edges.iterator();
                for (int j = 0; j < tryPerVM; j++) {
                    Switch dstSwitch = null;
                    {
                        while (itr.hasNext()) {
                            dstSwitch = itr.next();
                            if (dstSwitch.getUsedResources(dstSwitch.getState()) < 1) {//at least the forwarding rule
                                int pathLength = topology.getPathLength(dstSwitch, srcSwitch);
                                if (pathLength > 4) {
                                    //if (pathLength <=4 && pathLength>2) {
                                    break;
                                }
                            }
                        }
                        if (!itr.hasNext()) {
                            dstSwitch = edges.get(0);
                        }
                    }
//                final Map.Entry<Switch, Collection<Partition>> dst = getRandomEntry(sourcePartitions.entrySet());
                    final long vmIP = partition.getProperty(srcIPIndex).getStart();
                    if (migrationSteps.containsKey(vmIP)) {
                        continue;
                    }
                    //first check feasiblity
                    System.out.println("partition try " + tryNum + " switch " + j + " " + partition + " from " + srcSwitch + " to " + dstSwitch);
                    final Switch.FeasibleState oldSrcState = srcSwitch.getState();
                    final Switch.FeasibleState oldDstState = dstSwitch.getState();
                    try {
                        prepareMigration(partition, srcSwitch, dstSwitch);
                        //src
                        refillSwitch(sourcePartitions, forwardingRules, rplacement.get(srcSwitch), topology, srcSwitch);
                        //dst
                        refillSwitch(sourcePartitions, forwardingRules, rplacement.get(dstSwitch), topology, dstSwitch);
                        //// check if it is interesting
                        if (!isNewFeasible(partition, dstSwitch, false)) {
                            successful = true;
                            migrationSteps.put(vmIP, new MigrationStep(partition, srcSwitch, dstSwitch));
//                    migrationTest.put(srcSwitch,dstSwitch);
                            System.out.println("Migrating " + partition + " from " + srcSwitch + " to " + dstSwitch);
                        }else{
                            System.out.println("not interesting");
                            prepareMigration(partition, dstSwitch, srcSwitch);
                            srcSwitch.setState(oldSrcState);
                            dstSwitch.setState(oldDstState);
                        }

                    } catch (Switch.InfeasibleStateException e) {
                        System.out.println(e);
                        prepareMigration(partition, dstSwitch, srcSwitch);
                        srcSwitch.setState(oldSrcState);
                        dstSwitch.setState(oldDstState);
                    }
                }
            }
        }
        migrationStart = System.currentTimeMillis();
        //update mintraffics and traffic
        Thread[] threads = new Thread[threadNum];
        final Iterator<Map.Entry<Partition, Map<Rule, Collection<Flow>>>> iterator = ruleFlowMap.entrySet().iterator();
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new UpdateFlows(iterator, migrationSteps, minTraffics, topology);
        }
        Util.runThreads(threads);

        /*for (Map.Entry<Switch, Switch> entry : migrationTest.entrySet()) {

            int n = FeasiblePlacer.flowsNum(entry.getKey(), rplacement.get(entry.getKey()), topology);
            if (n!= ((OVSSwitch.OVSState) entry.getKey().getState()).getNewFlows()){
                System.out.println();
            }

             n = FeasiblePlacer.flowsNum(entry.getValue(), rplacement.get(entry.getValue()), topology);
            if (n!= ((OVSSwitch.OVSState) entry.getValue().getState()).getNewFlows()){
                System.out.println();
            }
        }*/

        return assignment.getPlacement();//rplacement does not change
    }

    private void prepareMigration(Partition partition, Switch srcSwitch, Switch dstSwitch) {
        for (SwitchHelper<? extends Switch> switchHelper : topology.getSwitchHelpers().values()) {
            switchHelper.migrate(srcSwitch, dstSwitch, partition);
        }
        {
            sourcePartitions.get(srcSwitch).remove(partition);
            Collection<Partition> partitions = sourcePartitions.get(dstSwitch);
            if (partitions == null) {
                partitions = new HashSet<>();
                sourcePartitions.put(dstSwitch, partitions);
            }
            partitions.add(partition);
        }
        final Rule rule = forwardingRules.get(partition).remove(srcSwitch);
        forwardingRules.get(partition).put(dstSwitch, rule);
    }


    private void refillSwitch(Map<Switch, Collection<Partition>> sourcePartitions, Map<Partition, Map<Switch, Rule>> forwardingRules,
                              Collection<Partition> rplacement, Topology topology, Switch aSwitch) throws Switch.InfeasibleStateException {
        final Switch.FeasibleState feasibleState = topology.getHelper(aSwitch).initToNotOnSrc(aSwitch, sourcePartitions.get(aSwitch), false);
        aSwitch.setState(feasibleState);
        if (rplacement != null) {
            topology.getHelper(aSwitch).isAddMultipleFeasible(aSwitch, rplacement, forwardingRules, true);
        }
    }

    private <T> T getRandomEntry(Collection<T> input) {
        int machineIndex = random.nextInt(input.size());
        final Iterator<T> itr = input.iterator();
        for (int j = 0; j < machineIndex - 1; j++) {
            itr.next();
        }
        return itr.next();
    }


    @Override
    public String toString() {
        return "VMStart Migrator";
    }

    /**
     * TOPOLOGY MUST BE RESTARTED BEFORE OR AFTER
     */
    public void restart() {
        final ListIterator<Map.Entry<Long, MigrationStep>> entryListIterator = new ArrayList<>(migrationSteps.entrySet()).listIterator(migrationSteps.size());
        while (entryListIterator.hasPrevious()) {
            Map.Entry<Long, MigrationStep> entry = entryListIterator.previous();
            entry.getValue().reverse();
            Switch srcSwitch = entry.getValue().src;
            Switch dstSwitch = entry.getValue().dst;
            Partition partition = entry.getValue().partition;
            prepareMigration(partition, srcSwitch, dstSwitch);//it is reversed before
            //don't need to refill switches as they going to be restarted

            System.out.println("Migrating " + partition + " from " + srcSwitch + " to " + dstSwitch);
        }
        Thread[] threads = new Thread[threadNum];
        final Iterator<Map.Entry<Partition, Map<Rule, Collection<Flow>>>> iterator = ruleFlowMap.entrySet().iterator();
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new UpdateFlows(iterator, migrationSteps, minTraffics, topology);
        }
        Util.runThreads(threads);

    }

    private static class MigrationStep {
        private Partition partition;
        private Switch src;
        private Switch dst;

        public MigrationStep(Partition partition, Switch src, Switch dst) {
            this.partition = partition;
            this.src = src;
            this.dst = dst;
        }

        public void reverse() {
            Switch tempSwitch = src;
            src = dst;
            dst = tempSwitch;
        }

    }

    private static class UpdateFlows extends Thread {
        private final Iterator<Map.Entry<Partition, Map<Rule, Collection<Flow>>>> iterator;
        private final Map<Long, MigrationStep> migrationPlan;
        private final Map<Partition, Long> minTraffic;
        private final Topology topology;

        public UpdateFlows(Iterator<Map.Entry<Partition, Map<Rule, Collection<Flow>>>> iterator, Map<Long, MigrationStep> migrationPlan,
                           Map<Partition, Long> minTraffic, Topology topology) {
            this.iterator = iterator;
            this.migrationPlan = migrationPlan;
            this.minTraffic = minTraffic;
            this.topology = topology;
        }

        public Map<Partition, Long> getMinTraffic() {
            return minTraffic;
        }

        @Override
        public void run() {
            final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
            final int dstIPIndex = Util.getDimensionInfoIndex(Util.DST_IP_INFO);
            while (true) {
                Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry;
                synchronized (iterator) {
                    if (iterator.hasNext()) {
                        entry = iterator.next();
                    } else {
                        break;
                    }
                }
                long sum = 0;
                for (Map.Entry<Rule, Collection<Flow>> entry1 : entry.getValue().entrySet()) {
                    for (Flow flow : entry1.getValue()) {
                        final Long src = flow.getProperty(srcIPIndex);
                        final MigrationStep newSrc = migrationPlan.get(src);
                        if (newSrc != null) {
                            flow.setSource(newSrc.dst);
//                            System.out.println("src: " + flow + " " + newSrc.src + " to " + newSrc.dst);
                        }
                        final Long dst = flow.getProperty(dstIPIndex);
                        final MigrationStep newDst = migrationPlan.get(dst);
                        if (newDst != null) {
                            flow.setDestination(newDst.dst);
//                            System.out.println("dst: " + flow + " " + newDst.src + " to " + newDst.dst);
                        }
                        if (entry1.getKey().getAction().doAction(flow) != null) {
                            sum += topology.getPathLength(flow.getSource(), flow.getDestination()) * flow.getTraffic();
                        }
                    }
                }
                minTraffic.put(entry.getKey(), sum);
            }
        }
    }
}
