package edu.usc.enl.cacheflow.algorithms.replication.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/9/12
 * Time: 9:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class ThreadMinTrafficSwitchReplicateSelectionPartition extends SwitchReplicateSelectionPartition {

    public Map<Switch, Integer> tieBreaker;
    public List<Switch> switches;
    private int threadNum;
    private ConcurrentMap<Switch, Long> switchTrafficMap;
    private Comparator<Switch> comparator;
    private ProcessThread[] threads;
    private Map<Switch, Map<Switch, Switch>> replicaSourceSelectedReplica;
    private List<Switch> sortedOutput;


    public ThreadMinTrafficSwitchReplicateSelectionPartition(int threadNum,Map<Partition, Map<Rule, List<Flow>>> ruleFlowMap) {
        super(ruleFlowMap);
        this.threadNum = threadNum;
        threads = new ProcessThread[threadNum];
    }

    @Override
    public void init( Topology topology) {
        super.init( topology);
        switches = new ArrayList<Switch>(topology.getSwitches());
        tieBreaker = new HashMap<Switch, Integer>(switches.size(), 1);
        switchTrafficMap = new ConcurrentHashMap<Switch, Long>(switches.size(), 1);
        sortedOutput = new ArrayList<Switch>(switches.size());
        for (Switch aSwitch : switches) {
            switchTrafficMap.put(aSwitch, Long.MAX_VALUE);
            sortedOutput.add(aSwitch);
        }
        replicaSourceSelectedReplica = new ConcurrentHashMap<Switch, Map<Switch, Switch>>();
        //tempOutputList = new ArrayList<Switch>(switches.size());
        comparator = new Comparator<Switch>() {
            public int compare(Switch o1, Switch o2) {
                final int i1 = switchTrafficMap.get(o1).compareTo(switchTrafficMap.get(o2));
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
    public Map<Switch, Switch> getSelectedReplicaIf(Switch replica) {
        return replicaSourceSelectedReplica.get(replica);
    }

    @Override
    public Long getTrafficIf(Switch replica) {
        return switchTrafficMap.get(replica);
    }

    @Override
    public List<Switch> sortSwitches(Collection<Switch> availableToChoose, Partition partition, Collection<Switch> sources,
                                     Collection<Switch> currentReplicas) {
        //place partition on each switch and compute traffic
        if (availableToChoose.size() == 0) {
            return new LinkedList<Switch>();
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
        replicaSourceSelectedReplica.clear();

        final Iterator<Switch> itr = availableToChoose.iterator();
        for (int j = 0; j < threadNum; j++) {
            threads[j] = new ProcessThread(itr, partition,
                    switchTrafficMap, topology, ruleFlowMap, replicaSourceSelectedReplica, currentReplicas, sources);
        }

        Util.runThreads(threads);

        /* LinkedList<Switch> output = new LinkedList<Switch>(switchTrafficMap.keySet());
        Iterator<Switch> itr = output.iterator();
        while (itr.hasNext()) {
            Switch next = itr.next();
            if (switchTrafficMap.get(next) >= previousOverhead) {
                itr.remove();//spurious replica
            }
        }*/
        Collections.sort(sortedOutput, comparator);
        for (int j = 0; j < threads.length; j++) {
            threads[j] = null;
        }
        return sortedOutput;
    }


    static class ProcessThread extends Thread {
        private final Iterator<Switch> itr;
        private final Partition partition;
        private final Map<Switch, Long> switchTrafficMap;
        private final Map<Partition, Map<Rule, List<Flow>>> ruleFlowMap;
        private final Topology topology;
        private final Switch[] currentReplicas;
        private final Map<Switch, Map<Switch, Switch>> replicaSourceSelectedReplica;
        private final Collection<Switch> sources;

        public ProcessThread(Iterator<Switch> itr, Partition partition,
                             Map<Switch, Long> switchTrafficMap,
                             Topology topology, Map<Partition, Map<Rule, List<Flow>>> ruleFlowMap,
                             Map<Switch, Map<Switch, Switch>> replicaSourceSelectedReplica, Collection<Switch> currentReplicas,
                             Collection<Switch> sources) {
            this.itr = itr;
            this.partition = partition;
            this.switchTrafficMap = switchTrafficMap;
            this.topology = topology;
            this.ruleFlowMap = ruleFlowMap;
            this.currentReplicas = new Switch[currentReplicas.size() + 1];
            int i = 0;
            for (Switch currentReplica : currentReplicas) {
                this.currentReplicas[i++] = currentReplica;
            }
            this.replicaSourceSelectedReplica = replicaSourceSelectedReplica;
            this.sources = sources;
        }

        @Override
        public void run() {
            //Map<Switch, Switch> sourceReplicaMap = new HashMap<Switch, Switch>();
            while (true) {
                Switch candidateSwitch;
                synchronized (itr) {
                    if (itr.hasNext()) {
                        candidateSwitch = itr.next();
                    } else {
                        break;
                    }
                }
                long traffic = 0;
                if (partition == null || partition.getRules() == null) {
                    System.out.println("null");
                }
                currentReplicas[currentReplicas.length - 1] = candidateSwitch;
                //fillNearestReplica(sources, currentReplicas, sourceReplicaMap, topology);
                HashMap<Switch, Switch> sourceReplicaMap = new HashMap<Switch, Switch>();
                for (Switch source : sources) {
                    long minReplicaTraffic = -1;
                    Switch minReplica = null;
                    for (Switch replica : currentReplicas) {
                        long sourceTrafficOnThisReplica = 0;
                        for (Map.Entry<Rule, List<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
                            for (Flow flow : entry.getValue()) {
                                if (flow.getSource().equals(source)) {
                                    sourceTrafficOnThisReplica += topology.getPathLength(flow.getSource(), replica) * flow.getTraffic();
                                    if (entry.getKey().getAction().doAction(flow) != null) {
                                        sourceTrafficOnThisReplica += topology.getPathLength(replica, flow.getDestination()) * flow.getTraffic();
                                    }
                                }
                            }
                        }
                        if (minReplica == null || minReplicaTraffic > sourceTrafficOnThisReplica) {
                            minReplica = replica;
                            minReplicaTraffic = sourceTrafficOnThisReplica;
                        }
                    }
                    sourceReplicaMap.put(source, minReplica);
                    traffic += minReplicaTraffic;
                }


                /*for (Map.Entry<Switch, Map<Action, List<Flow>>> entry : sourceTraffic.entrySet()) {
                    double minReplicaTraffic = -1;
                    Switch minReplica = null;
                    for (Switch replica : currentReplicas) {
                                        double sourceTrafficOnThisReplica = 0;
                        for (Map.Entry<Action, List<Flow>> entry2 : entry.getValue().entrySet()) {
                            Action action = entry2.getKey();
                            for (Flow flow : entry2.getValue()) {
                                sourceTrafficOnThisReplica += topology.getPathLength(flow.getSource(), replica) * flow.getTraffic();
                                if (action.doAction(flow) != null) {
                                    sourceTrafficOnThisReplica += topology.getPathLength(replica, flow.getDestination()) * flow.getTraffic();
                                }
                            }
                        }
                        if (minReplica == null || minReplicaTraffic > sourceTrafficOnThisReplica) {
                            minReplica = replica;
                            minReplicaTraffic = sourceTrafficOnThisReplica;
                        }
                    }
                    Switch source = entry.getKey();
                    sourceReplicaMap.put(source, minReplica);
                    traffic += minReplicaTraffic;
                }*/
                switchTrafficMap.put(candidateSwitch, traffic);
                replicaSourceSelectedReplica.put(candidateSwitch, sourceReplicaMap);
            }
        }
    }
}
