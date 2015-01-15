package edu.usc.enl.cacheflow.algorithms;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 5:19 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Placer {
    protected Topology topology;
    protected final Map<Partition, Map<Switch, Rule>> forwardingRules;
    protected final Map<Switch, Collection<Partition>> sourcePartitions;
    protected final boolean checkLinks;
    protected final int threadNum;
    protected long start;

    protected Placer(boolean checkLinks, Map<Partition, Map<Switch, Rule>> forwardingRules, Map<Switch, Collection<Partition>> sourcePartitions, int threadNum) {

        this.checkLinks = checkLinks;
        this.forwardingRules = forwardingRules;
        this.sourcePartitions = sourcePartitions;
        this.threadNum = threadNum;
    }

    protected void updateLinks(Map<Link, Long> oldTraffic, Map<Link, Long> linkTraffic) {
        for (Map.Entry<Link, Long> linkDoubleEntry : linkTraffic.entrySet()) {
            Link link = linkDoubleEntry.getKey();
            if (oldTraffic != null) {
                oldTraffic.put(link, link.getUsedCapacity());
            }
            link.setUsedCapacity(linkDoubleEntry.getValue());
        }
    }

    public abstract Collection<Switch> getLastAvailableSwitches();

    protected boolean linkFails(Map<Link, Long> linkTraffic) {
        for (Map.Entry<Link, Long> linkUsedCapacityEntry : linkTraffic.entrySet()) {
            Link link = linkUsedCapacityEntry.getKey();
            if (link.getCapacity() < linkTraffic.get(link)) {
                Util.logger.info(link.toString() + " failed. " + linkTraffic.get(link) + ">" + link.getCapacity());
                return true;
            }
        }
        return false;
    }

    public  Statistics getStats(Map<String,Object> parameters){
        Statistics stat = new Statistics();
        stat.setParameters(parameters);
        stat.addStat("placementDuration", System.currentTimeMillis()-start);
        return stat;
    }


    public final Assignment place(Topology topology, Collection<Partition> partitions) throws NoAssignmentFoundException {
        init( topology);
        start = System.currentTimeMillis();
        return place2( topology, partitions);
    }

    public abstract Assignment place2(Topology topology, Collection<Partition> partitions) throws NoAssignmentFoundException;

    protected void init( Topology topology) {
        this.topology = topology;
    }

    protected String getIndent(int level) {
        char[] indentChar = new char[level * 2];
        Arrays.fill(indentChar, ' ');
        return String.copyValueOf(indentChar);
    }

    protected void pruneSwitches(List<Switch> availableSwitches) {
        //prune switches
        Iterator<Switch> iterator = availableSwitches.iterator();
        while (iterator.hasNext()) {
            Switch availableSwitch = iterator.next();
            if (!availableSwitch.canSaveMore()) {
                iterator.remove();
            }
        }
    }

    protected void revert(Map<Link, Long> oldTraffic, Map<Switch, Switch.FeasibleState> oldSwitchStates) {
        for (Map.Entry<Link, Long> linkUsedCapacity : oldTraffic.entrySet()) {
            linkUsedCapacity.getKey().setUsedCapacity(linkUsedCapacity.getValue());
        }

        for (Map.Entry<Switch, Switch.FeasibleState> entry : oldSwitchStates.entrySet()) {
            entry.getKey().setState(entry.getValue());
        }
    }

    protected void setInitialState(Collection<Partition> partitionSpace) throws Switch.InfeasibleStateException {
        final List<Switch> edges = topology.findEdges();
        for (Switch edge : edges) {
            topology.getHelper(edge).initToNotOnSrc(edge,sourcePartitions.get(edge), true);
        }

        partitionSpace.retainAll(forwardingRules.keySet());
    }

    protected class StackObjects<T> {
        private List<Switch> availableSwitches;
        private LinkedList<Switch> availableToChoose;
        private T candidateCluster;
        private Map<Switch, Switch.FeasibleState> oldSwitchStates = new HashMap<Switch, Switch.FeasibleState>();
        private Map<Link, Long> oldTraffic = new HashMap<Link, Long>();

        public StackObjects(List<Switch> availableSwitches) {
            this.availableSwitches = availableSwitches;
        }

        public void setAvailableToChoose(LinkedList<Switch> availableToChoose) {
            this.availableToChoose = availableToChoose;
        }

        public List<Switch> getAvailableSwitches() {
            return availableSwitches;
        }

        public LinkedList<Switch> getAvailableToChoose() {
            return availableToChoose;
        }

        public Map<Link, Long> getOldTraffic() {
            return oldTraffic;
        }

        public T getCandidateObject() {
            return candidateCluster;
        }

        public void setCandidateObject(T candidateCluster) {
            this.candidateCluster = candidateCluster;
        }

        public Map<Switch, Switch.FeasibleState> getOldSwitchStates() {
            return oldSwitchStates;
        }

        public void empty() {
            availableSwitches = null;
            availableToChoose = null;
            oldSwitchStates = null;
            oldTraffic = null;
        }
    }
}
