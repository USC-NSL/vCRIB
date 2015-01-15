package edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
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
public class ThreadMinTrafficSwitchSelection extends TrafficAwareSwitchSelection {
    public ProcessThread[] threads;

    public ThreadMinTrafficSwitchSelection( int threadNum) {
        threads = new ProcessThread[threadNum];
    }

    @Override
    public void init(Topology topology) {
        super.init(topology);
        switches = new ArrayList<Switch>(topology.getSwitches());
        tieBreaker = new HashMap<Switch, Integer>(switches.size(), 1);
        switchTrafficMap = new ConcurrentHashMap<Switch, Long>(switches.size(), 1);
        for (Switch aSwitch : switches) {
            switchTrafficMap.put(aSwitch, Long.MAX_VALUE);
        }
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
    public <T extends List<Switch>> T sortSwitches(T toFill, Map<Partition, Switch> placement,
                                                   Partition partition) {
        //place partition on each switch and compute traffic
        if (toFill.size() == 0) {
            return toFill;
        }
        for (Map.Entry<Switch, Long> entry : switchTrafficMap.entrySet()) {
            entry.setValue(Long.MAX_VALUE);
        }

        //tieBreaker.clear(); no need to clear
        int i = 0;
        Collections.shuffle(switches, Util.random);
        for (Switch aSwitch : switches) {
            tieBreaker.put(aSwitch, i++);
        }


        final Iterator<Switch> itr = toFill.iterator();
        for (int j = 0; j < threads.length; j++) {
            threads[j] = new ProcessThread(itr, partition,
                    switchTrafficMap, topology);
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
        private final Iterator<Switch> itr;
        private final Partition partition;
        private final Map<Switch, Long> switchTrafficMap;
        private final Topology topology;

        public ProcessThread(Iterator<Switch> itr, Partition partition,
                             Map<Switch, Long> switchTrafficMap,
                             Topology topology) {
            this.itr = itr;
            this.partition = partition;
            this.switchTrafficMap = switchTrafficMap;
            this.topology = topology;
        }

        @Override
        public void run() {
            while (true) {
                Switch candidateSwitch;
                synchronized (itr) {
                    if (itr.hasNext()) {
                        candidateSwitch = itr.next();
                    } else {
                        break;
                    }
                }
                long traffic =topology.getTrafficForHosting(partition,candidateSwitch);
                switchTrafficMap.put(candidateSwitch, traffic);
            }
        }
    }
}
