package edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.TCAMSRAMSwitch;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/31/11
 * Time: 6:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class MinTrafficSameRuleSwitchSelection extends TrafficAwareSwitchSelection {
    protected Map<Switch, Integer> sameRuleNumMap;
    protected final CollectionPool<Set<Rule>> ruleSetPool;


    public MinTrafficSameRuleSwitchSelection(CollectionPool<Set<Rule>> ruleSetPool) {
        this.ruleSetPool = ruleSetPool;
    }

    @Override
    public void init(Topology topology) {
        super.init(topology);
        switches = new ArrayList<Switch>(topology.getSwitches());
        tieBreaker = new HashMap<Switch, Integer>(switches.size(), 1);
        switchTrafficMap = new HashMap<Switch, Long>(switches.size(), 1);
        sameRuleNumMap = new HashMap<Switch, Integer>(switches.size(), 1);
        for (Switch aSwitch : switches) {
            switchTrafficMap.put(aSwitch, Long.MAX_VALUE);
            sameRuleNumMap.put(aSwitch, -1);
        }
        comparator = new SwitchComparator();
    }

    @Override
    public String toString() {
        return "Min Same Traffic";
    }

    @Override
    public <T extends List<Switch>> T sortSwitches(T toFill, Map<Partition, Switch> placement,
                                                   final Partition partition) {
        //place partition on each switch and compute traffic
        if (toFill.size() == 0) {
            return toFill;
        }
        ((SwitchComparator) comparator).setPartition(partition);
        //switchTrafficMap.clear();
        for (Map.Entry<Switch, Long> entry : switchTrafficMap.entrySet()) {
            entry.setValue(Long.MAX_VALUE);
        }
        //tieBreaker.clear();
        //sameRuleNumMap.clear();
        for (Map.Entry<Switch, Integer> entry : sameRuleNumMap.entrySet()) {
            entry.setValue(-1);
        }
        int i = 0;
        Collections.shuffle(switches, Util.random);
        for (Switch aSwitch : switches) {
            tieBreaker.put(aSwitch, i++);
        }

        for (Switch candidateSwitch : toFill) {
            long traffic = topology.getTrafficForHosting(partition, candidateSwitch);
            switchTrafficMap.put(candidateSwitch, traffic);
        }


        Collections.sort(toFill, comparator);
        return toFill;
    }

    private Integer getTieBreakerValueFor(Switch host, Partition p, Map<Switch, Integer> tieBreakerMap) {
        final Integer value = tieBreakerMap.get(host);
        if (value != null) {
            return value;
        }
        final Collection<Rule> pRules = p.getRules();
        int sum = getSimilarity(host, pRules, ruleSetPool);
        tieBreakerMap.put(host, sum);
        return sum;
    }

    public static int getSimilarity(Switch host, Collection<Rule> pRules, CollectionPool<Set<Rule>> ruleSetPool) {
        int sum = 0;
        final CollectionPool.TempCollection<Set<Rule>> tempCollection = ruleSetPool.getTempCollection();
        final Set<Rule> hostRules = tempCollection.getData();
        host.getState().getRules(hostRules);
        if (pRules instanceof MatrixRuleSet) {
            sum = ((MatrixRuleSet) pRules).getSimilarity((MatrixRuleSet) hostRules);
        } else {
            for (Rule pRule : pRules) {
                if (hostRules.contains(pRule)) {
                    sum++;
                }
            }
        }
        tempCollection.release();
        return sum;
    }

    protected class SwitchComparator implements Comparator<Switch> {
        Partition partition;

        public void setPartition(Partition partition) {
            this.partition = partition;
        }

        public int compare(Switch o1, Switch o2) {
            final int i1 = switchTrafficMap.get(o1).compareTo(switchTrafficMap.get(o2));
            if (i1 == 0 && !o1.equals(o2)) {
                final int output = getTieBreakerValueFor(o2, partition, sameRuleNumMap) - getTieBreakerValueFor(o1, partition, sameRuleNumMap);
                if (output == 0 && !o1.equals(o2)) {
                    return tieBreaker.get(o2) - tieBreaker.get(o1);
                }
                return output;
            }
            return i1;
        }
    }
}
