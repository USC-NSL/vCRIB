package edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/31/11
 * Time: 6:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class ThreadMinTrafficSameRuleSwitchSelection extends MinTrafficSameRuleSwitchSelection{
    private ThreadMinTrafficSwitchSelection.ProcessThread[] threads;

    public ThreadMinTrafficSameRuleSwitchSelection( CollectionPool<Set<Rule>> ruleSetPool,int threadNum) {
        super( ruleSetPool);
        threads = new ThreadMinTrafficSwitchSelection.ProcessThread[threadNum];
    }

    @Override
    public void init(Topology topology) {
        super.init(topology);
        switches = new ArrayList<Switch>(topology.getSwitches());
        tieBreaker = new HashMap<Switch, Integer>(switches.size(), 1);
        switchTrafficMap = new ConcurrentHashMap<Switch, Long>(switches.size(), 1);
        sameRuleNumMap = new ConcurrentHashMap<Switch, Integer>(switches.size(), 1);
        for (Switch aSwitch : switches) {
            switchTrafficMap.put(aSwitch, Long.MAX_VALUE);
            sameRuleNumMap.put(aSwitch, -1);
        }
        comparator = new SwitchComparator();
    }

    @Override
    public String toString() {
        return "Thread Min Same Traffic";
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
        //ALTHOUGTHH TO FILL TOFILL YOU DO NOT NEED THIS BUT GETTRAFFICMAP NEEDS THESE TO BE MAX
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

        final Iterator<Switch> itr = toFill.iterator();
        for (int j = 0; j < threads.length; j++) {
            threads[j] = new ThreadMinTrafficSwitchSelection.ProcessThread(itr, partition,
                    switchTrafficMap, topology);
            threads[j].start();
        }

        try {
            for (ThreadMinTrafficSwitchSelection.ProcessThread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Collections.sort(toFill, comparator);
        return toFill;
    }

}
