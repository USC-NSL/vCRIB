package edu.usc.enl.cacheflow.algorithms.placement.switchselectioncluster;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/31/11
 * Time: 6:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class ThreadMinTrafficSwitchSelectionCluster extends SwitchSelectionCluster {
    private Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;
    public Map<Switch, Integer> tieBreaker;
    public List<Switch> switches;
    private int threadNum;
    public ConcurrentMap<Switch, Long> switchTrafficMap;
    private Comparator<Switch> comparator;
    public ProcessThread[] threads;
    //private List<Switch> tempOutputList;


    public ThreadMinTrafficSwitchSelectionCluster(int threadNum) {
        this.threadNum = threadNum;
        threads = new ProcessThread[threadNum];
    }

    @Override
    public void init(Topology topology) {
        super.init(topology);
        this.ruleFlowMap = ruleFlowMap;
        switches = new ArrayList<Switch>(topology.getSwitches());
        tieBreaker = new HashMap<Switch, Integer>(switches.size(), 1);
        switchTrafficMap = new ConcurrentHashMap<Switch, Long>(switches.size(), 1);
        for (Switch aSwitch : switches) {
            switchTrafficMap.put(aSwitch, Long.MAX_VALUE);
        }
        //tempOutputList = new ArrayList<Switch>(switches.size());
        comparator = new Comparator<Switch>() {
            public int compare(Switch o1, Switch o2) {
                final int i1 =  switchTrafficMap.get(o1).compareTo( switchTrafficMap.get(o2));
                if (i1 == 0 && !o1.equals(o2)) {
                    return tieBreaker.get(o1) - tieBreaker.get(o2);
                }
                return i1;
            }
        };

    }

    @Override
    public String toString() {
        return "Thread Min Traffic";
    }

    @Override
    public  <T extends List<Switch>> T sortSwitches(T toFill, Map<Cluster, Switch> assignment,
                                           Cluster cluster) {
        //place partition on each switch and compute traffic
        if (toFill.size() == 0) {
            return toFill;
        }
        //switchTrafficMap.clear();
        for (Map.Entry<Switch, Long> entry : switchTrafficMap.entrySet()) {
            entry.setValue(Long.MAX_VALUE);
        }

        //tieBreaker.clear(); no need to clear
        int i = 0;
        Collections.shuffle(switches, Util.random);
        for (Switch aSwitch : switches) {
            tieBreaker.put(aSwitch, i++);
        }


        for (int j = 0; j < threadNum; j++) {
            threads[j] = new ProcessThread();
        }
        int switchPerThread = (int) (Math.ceil(1.0 * toFill.size() / threads.length));
        for (int j = 0; j < threads.length; j++) {
            threads[j].init(toFill, j * switchPerThread,
                    Math.min(toFill.size(), (j + 1) * switchPerThread), cluster,
                    switchTrafficMap, topology, ruleFlowMap);
            threads[j].start();
        }

        try {
            for (ProcessThread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Collections.sort(toFill, comparator);
        return toFill;
    }

    static class ProcessThread extends Thread {
        private int start;
        private int finish;
        private List<Switch> availableToChoose;
        private Cluster cluster;
        private Map<Switch, Long> switchTrafficMap;
        private Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;
        private Topology topology;

        public void init(List<Switch> availableToChoose, int start, int finish, Cluster cluster,
                         Map<Switch, Long> switchTrafficMap,
                         Topology topology, Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {
            this.availableToChoose = availableToChoose;
            this.start = start;
            this.finish = finish;
            this.cluster = cluster;
            this.switchTrafficMap = switchTrafficMap;
            this.topology = topology;
            this.ruleFlowMap = ruleFlowMap;
        }

        @Override
        public void run() {
            for (int i = start; i < finish; i++) {
                Switch candidateSwitch = availableToChoose.get(i);
                long traffic = 0;
                if (cluster == null || cluster.getRules() == null) {
                    System.out.println("null");
                }
                for (Partition partition : cluster.getPartitions()) {
                    Map<Rule, Collection<Flow>> ruleFlowMap2 = ruleFlowMap.get(partition);
                    for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap2.entrySet()) {
                        for (Flow flow : entry.getValue()) {
                            traffic += topology.getPathLength(flow.getSource(), candidateSwitch) * flow.getTraffic();
                            if (entry.getKey().getAction().doAction(flow) != null) {
                                traffic += topology.getPathLength(candidateSwitch, flow.getDestination()) * flow.getTraffic();
                            }
                        }
                    }
                }
                switchTrafficMap.put(candidateSwitch, traffic);
            }
        }
    }
}
