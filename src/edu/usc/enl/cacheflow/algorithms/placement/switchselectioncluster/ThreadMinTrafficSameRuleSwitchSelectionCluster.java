package edu.usc.enl.cacheflow.algorithms.placement.switchselectioncluster;

import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.MinTrafficSameRuleSwitchSelection;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
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
public class ThreadMinTrafficSameRuleSwitchSelectionCluster extends SwitchSelectionCluster {
    private Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;
    public Map<Switch, Integer> tieBreaker2;
    public Map<Switch, Integer> sameRuleNumMap;
    public Map<Switch, Long> switchTrafficMap;
    public final SwitchComparator comparator;
    public List<Switch> switches;
    //private List<Switch> tempOutputList;
    private int threadNum;
    protected final CollectionPool<Set<Rule>> ruleSetPool;

    public ThreadMinTrafficSameRuleSwitchSelectionCluster(int threadNum, CollectionPool<Set<Rule>> ruleSetPool) {
        this.ruleSetPool = ruleSetPool;
        comparator = new SwitchComparator();
        this.threadNum = threadNum;
    }

    @Override
    public void init(Topology topology) {
        super.init(topology);
        this.ruleFlowMap = ruleFlowMap;
        switches = new ArrayList<Switch>(topology.getSwitches());
        tieBreaker2 = new HashMap<Switch, Integer>(switches.size(), 1);
        switchTrafficMap = new ConcurrentHashMap<Switch, Long>(switches.size(), 1);
        sameRuleNumMap = new ConcurrentHashMap<Switch, Integer>(switches.size(), 1);
        for (Switch aSwitch : switches) {
            switchTrafficMap.put(aSwitch, Long.MAX_VALUE);
            sameRuleNumMap.put(aSwitch, -1);
        }
        //tempOutputList = new ArrayList<Switch>(switches.size());
    }

    @Override
    public String toString() {
        return "Thread Min Same Traffic";
    }

    @Override
    public  <T extends List<Switch>> T sortSwitches(T toFill, Map<Cluster, Switch> assignment,
                                           final Cluster cluster) {
        //place partition on each switch and compute traffic
        if (toFill.size() == 0) {
            return toFill;
        }
        comparator.setCluster(cluster);
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
            tieBreaker2.put(aSwitch, i++);
        }

        final List<ThreadMinTrafficSwitchSelectionCluster.ProcessThread> threads;
        threads = new ArrayList<ThreadMinTrafficSwitchSelectionCluster.ProcessThread>(threadNum);
        for (int j = 0; j < threadNum; j++) {
            threads.add(new ThreadMinTrafficSwitchSelectionCluster.ProcessThread());
        }
        int switchPerThread = (int) (Math.ceil(1.0 * toFill.size() / threads.size()));
        for (int j = 0; j < threads.size(); j++) {
            threads.get(j).init(toFill, j * switchPerThread,
                    Math.min(toFill.size(), (j + 1) * switchPerThread), cluster,
                    switchTrafficMap, topology, ruleFlowMap);
            threads.get(j).start();
        }

        try {
            for (ThreadMinTrafficSwitchSelectionCluster.ProcessThread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Collections.sort(toFill, comparator);
        return toFill;
    }

    private Integer getTieBreakerValueFor(Switch host, Cluster c, Map<Switch, Integer> tieBreakerMap) {
        final Integer value = tieBreakerMap.get(host);
        if (value != null && value >= 0) {
            return value;
        }
        final Collection<Rule> cRules = c.getRules();
        final int sum = MinTrafficSameRuleSwitchSelection.getSimilarity(host, cRules, ruleSetPool);


        tieBreakerMap.put(host, sum);
        return sum;
    }

    private class SwitchComparator implements Comparator<Switch> {
        Cluster cluster;

        public void setCluster(Cluster cluster) {
            this.cluster = cluster;
        }

        public int compare(Switch o1, Switch o2) {
            final int i1 = switchTrafficMap.get(o1).compareTo(switchTrafficMap.get(o2));
            if (i1 == 0 && !o1.equals(o2)) {
                final int output = getTieBreakerValueFor(o2, cluster, sameRuleNumMap) - getTieBreakerValueFor(o1, cluster, sameRuleNumMap);
                if (output == 0 && !o1.equals(o2)) {
                    return tieBreaker2.get(o2) - tieBreaker2.get(o1);
                }
                return output;
            }
            return i1;
        }
    }

}
