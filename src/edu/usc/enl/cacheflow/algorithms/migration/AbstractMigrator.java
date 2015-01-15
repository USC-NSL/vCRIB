package edu.usc.enl.cacheflow.algorithms.migration;

import edu.usc.enl.cacheflow.algorithms.replication.Replicator;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.algorithms.PostPlacer;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/10/12
 * Time: 5:15 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractMigrator implements PostPlacer {
    protected final Map<Partition, Map<Switch, Rule>> forwardingRules;
    protected final Topology topology;
    protected final Map<Partition, Long> minTraffics;
    protected final Map<Switch, Collection<Partition>> sourcePartitions;
    protected final Map<Partition, Long> overhead;
    protected Map<Switch, Set<Partition>> rPlacement;
    protected long start;
    protected long migrationStart;
    protected long totalTime;
    private long migrationTime;
    private int changesNum;

    protected final Map<Class<? extends Switch>, Double> switchTypeResourceUsage;

    public AbstractMigrator(Topology topology,
                            Map<Partition, Long> minTraffics,
                            Map<Switch, Collection<Partition>> sourcePartitions,
                            Map<Partition, Map<Switch, Rule>> forwardingRules) {
        this.topology = topology;
        this.minTraffics = minTraffics;
        this.sourcePartitions = sourcePartitions;
        this.forwardingRules = forwardingRules;
        overhead = new HashMap<>(forwardingRules.size(), 1);
        switchTypeResourceUsage = new TreeMap<>(new Comparator<Class<? extends Switch>>() {
            @Override
            public int compare(Class<? extends Switch> o1, Class<? extends Switch> o2) {
                return o1.getSimpleName().compareTo(o2.getSimpleName());
            }
        });
    }

    protected double initCurrentResourceUsage() {
        double currentResourceUsage=0;
        switchTypeResourceUsage.clear();
        for (Switch aSwitch : topology.getSwitches()) {
            currentResourceUsage += aSwitch.getUsedResources(aSwitch.getState());
            Double usage = switchTypeResourceUsage.get(aSwitch.getClass());
            if (usage == null) {
                usage = 0d;
            }
            switchTypeResourceUsage.put(aSwitch.getClass(), usage + aSwitch.getUsedAbsoluteResources(aSwitch.getState()));
        }
        return currentResourceUsage;
    }

    @Override
    public final Map<Partition, Map<Switch, Switch>> postPlace(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter) {
        rPlacement = assignment.getRplacement();
        start = System.currentTimeMillis();
        final Map<Partition, Switch> originalPlacement = new HashMap<>(assignment.getPlacement());
        Map<Partition, Switch> placement = postPlace2(availableSwitches, assignment, trendWriter);
        changesNum = 0;
        for (Map.Entry<Partition, Switch> entry : originalPlacement.entrySet()) {
            if (!entry.getValue().equals(placement.get(entry.getKey()))) {
                changesNum++;
            }
        }
        return createOutput(placement);
    }

    public abstract Map<Partition, Switch> postPlace2(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter);

    public String getSwitchTypeResourceUsage() {
        StringBuilder sb = new StringBuilder();
        for (Double aDouble : switchTypeResourceUsage.values()) {
            sb.append(aDouble).append(",");
        }
        return sb.toString();
    }

    protected Map<Partition, Map<Switch, Switch>> createOutput(Map<Partition, Switch> placement) {
        long finishTime = System.currentTimeMillis();
        totalTime = finishTime - start;
        migrationTime = finishTime - migrationStart;
        Map<Partition, Map<Switch, Switch>> output = new HashMap<Partition, Map<Switch, Switch>>();
        for (Map.Entry<Switch, Collection<Partition>> entry : sourcePartitions.entrySet()) {
            for (Partition partition : entry.getValue()) {
                Map<Switch, Switch> srcHostMap = output.get(partition);
                if (srcHostMap == null) {
                    srcHostMap = new HashMap<Switch, Switch>();
                    output.put(partition, srcHostMap);
                }
                srcHostMap.put(entry.getKey(), placement.get(partition));
            }
        }
        Replicator.updateForwardingRules(output, forwardingRules);
        return output;
    }

    @Override
    public Statistics getStats(Map<String, Object> parameters) {
        int num = 0;
        for (Long aLong : overhead.values()) {
            if (aLong == 0) {
                num++;
            }
        }
        Statistics stat = new Statistics();
        stat.setParameters(parameters);
        stat.addStat("0overhead partitions", num);
        stat.addStat("migrationDuration", totalTime);
        stat.addStat("onlineDuration", migrationTime);
        stat.addStat("migrationChanges", changesNum);
        return stat;
    }

    protected Switch.FeasibleState isOldHostFeasible(Set<Partition> rPlacement, Partition partition, Switch oldHost,
                                                     boolean commit)
            throws Switch.InfeasibleStateException {
        Map<Partition, Collection<Switch>> currentPartitions = new HashMap<>(rPlacement.size(), 1);
        for (Partition partition1 : rPlacement) {
            currentPartitions.put(partition1, forwardingRules.get(partition1).keySet());
        }
        return topology.getHelper(oldHost).isRemoveFeasible(oldHost, currentPartitions, sourcePartitions.get(oldHost),
                true, commit, partition, forwardingRules.get(partition).keySet());
    }

    protected long computeInitialOverhead(Partition partition, Switch host, Long minOverhead) {

        return topology.getTrafficForHosting(partition, host) - minOverhead;
    }

    protected boolean isNewFeasible(Partition p, Switch s, boolean commit) {
        try {
            if (commit) {
                topology.getHelper(s).isAddFeasible(s, p, forwardingRules.get(p).keySet(), true, commit);
            } else {
                topology.getHelper(s).resourceUsage(s, p, forwardingRules.get(p).keySet(), true);
            }
        } catch (Switch.InfeasibleStateException e) {
            /*if (commit){
                System.out.println(e);
                try {
                    System.out.println(topology.getHelper(s).resourceUsage(s, p, forwardingRules.get(p).keySet()));
                    System.out.println(topology.getHelper(s).isAddFeasible(s, p, forwardingRules.get(p).keySet(), true, commit));

                } catch (Switch.InfeasibleStateException e1) {
                    System.out.println();
                }
            }*/
            return false;
        }
        return true;
    }


    protected List<Switch> getAllConsiderableSwitches(Collection<Partition> partitions) {
        List<Switch> allConsiderableSwitches = new ArrayList<Switch>(topology.getSwitches());
        {
            int minPartitionSize = -1;
            for (Partition partition : partitions) {
                if (minPartitionSize > partition.getSize() || minPartitionSize < 0) {
                    minPartitionSize = partition.getSize();
                }
            }
            for (Iterator<Switch> iterator = allConsiderableSwitches.iterator(); iterator.hasNext(); ) {
                Switch aSwitch = iterator.next();
                if (aSwitch instanceof MemorySwitch && ((MemorySwitch) aSwitch).getMemoryCapacity() < minPartitionSize) {
                    iterator.remove();
                }
            }
        }
        return allConsiderableSwitches;
    }
}
