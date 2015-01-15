package edu.usc.enl.cacheflow.algorithms.feasibility.general;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/22/12
 * Time: 12:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class InitialNewFlowsFiller extends Thread {
    private final Iterator<Partition> iterator;
    private final Map<Switch, Integer> switchNewFlows;
    private final Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;
    private final boolean addLocalFlows;

    /**
     *
     * @param iterator
     * @param switchNewFlows MUST BE SPECIFIC FOR THIS THREAD
     * @param ruleFlowMap
     * @param addLocalFlows
     */
    public InitialNewFlowsFiller(Iterator<Partition> iterator, Map<Switch, Integer> switchNewFlows,
                                 Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap, boolean addLocalFlows) {
        this.iterator = iterator;
        this.switchNewFlows = switchNewFlows;
        this.ruleFlowMap = ruleFlowMap;
        this.addLocalFlows = addLocalFlows;
    }

    public Map<Switch, Integer> getSwitchNewFlows() {
        return switchNewFlows;
    }

    @Override
    public void run() {
        while (true) {
            //PartitionObject partitionObject;
            Partition partition;
            synchronized (iterator) {
                if (iterator.hasNext()) {
                    partition = iterator.next();
                } else {
                    break;
                }
            }
//            int notSrcNewFlows = 0;
            for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
                for (Flow flow : entry.getValue()) {
                    final Switch source = flow.getSource();
                    final Switch destination = flow.getDestination();

                    switchNewFlows.put(source, switchNewFlows.get(source) + 1);
                    if (entry.getKey().getAction().doAction(flow) != null) {//an accept rule
//                        if (source.equals(destination)) {
//                            notSrcNewFlows++;
//                        }
                        //else { //TO START WITH NO SRC STATE BY DEFAULT
                        if (addLocalFlows || !source.equals(destination)) {
                            switchNewFlows.put(destination, switchNewFlows.get(destination) + 1);
                        }
                        // }
                    }
                }
            }
            //partitionObject.setNotSrcNewFlows(notSrcNewFlows);
        }
    }

    public static Map<Switch, Integer> run(int threadNum, Collection<Switch> switches, boolean addLocalFlows, Iterator<Partition> iterator,
                                           Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {
        final LinkedHashMap<Switch, Integer> switchNewFlows = new LinkedHashMap<>(switches.size(), 1);
        for (Switch edgeSwitch : switches) {
            switchNewFlows.put(edgeSwitch, 0);
        }

        InitialNewFlowsFiller[] threads = new InitialNewFlowsFiller[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new InitialNewFlowsFiller(iterator,
                    (i == 0 ? switchNewFlows : ((Map<Switch, Integer>) switchNewFlows.clone())), ruleFlowMap, addLocalFlows);
        }
        Util.runThreads(threads);
        //sum all maps
        for (int i = 1; i < threads.length; i++) {
            InitialNewFlowsFiller thread = threads[i];
            final Iterator<Map.Entry<Switch, Integer>> itr1 = switchNewFlows.entrySet().iterator();
            final Iterator<Map.Entry<Switch, Integer>> itr2 = thread.getSwitchNewFlows().entrySet().iterator();
            while (itr1.hasNext()) {
                final Map.Entry<Switch, Integer> next1 = itr1.next();
                final Map.Entry<Switch, Integer> next2 = itr2.next();
                next1.setValue(next1.getValue() + next2.getValue());
            }
        }
        return switchNewFlows;
    }
}
