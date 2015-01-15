package edu.usc.enl.cacheflow.algorithms.migration;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/10/12
 * Time: 10:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueuePartition implements Comparable<QueuePartition> {
    private Partition partition;
    protected boolean active;
    private long overhead;
    protected boolean onceBeenDeactivated = false;
    Map<Switch, CandidateSwitchData> candidateSwitches;
    protected boolean useMyMap = false;
    protected Comparator<Switch> comparator;

    public QueuePartition(Partition partition, boolean active, long overhead) {
        this.partition = partition;
        this.active = active;
        this.overhead = overhead;
    }

    @Override
    public String toString() {
        return "QueuePartition{" +
                "partition=" + partition +
                ", active=" + active +
                ", overhead=" + overhead +
                ", useMyMap=" + useMyMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueuePartition that = (QueuePartition) o;

        if (partition != null ? !partition.equals(that.partition) : that.partition != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return partition != null ? partition.hashCode() : 0;
    }

    public long getOverhead() {
        return overhead;
    }

    public Partition getPartition() {
        return partition;
    }

    @Override
    public int compareTo(QueuePartition o) {
        final int compare = Long.compare(o.overhead, overhead);
        if (compare == 0 && !partition.equals(o.partition)) {
            return partition.hashCode() - o.partition.hashCode();
        }
        return compare;
    }

    public boolean isActive() {
        return active;
    }

    public void activate(Switch oldHost) {
        if (candidateSwitches != null && overhead > 0) {
            final CandidateSwitchData candidateSwitchData = candidateSwitches.get(oldHost);
            if (candidateSwitchData != null) {
                candidateSwitchData.active = true;
                candidateSwitches.remove(oldHost);
                candidateSwitches.put(oldHost, candidateSwitchData);
                active = true;
            }
        }
    }

    public void reDeActivate(Collection<Switch> oldActiveSwitches) {
        for (Switch oldActiveSwitch : oldActiveSwitches) {
            candidateSwitches.get(oldActiveSwitch).active = false;
        }

        active = false;
        useMyMap = true;
        onceBeenDeactivated = true;
    }
    public void reDeActivate() {
        for (Map.Entry<Switch, CandidateSwitchData> entry : candidateSwitches.entrySet()) {
            entry.getValue().active=false;
        }

        active = false;
        useMyMap = true;
        onceBeenDeactivated = true;
    }

    public void deActivate(Map<Switch, Long> newCandidateSwitches, long oldTraffic) {

        keepNewCandidateSwitchesOnly(newCandidateSwitches, oldTraffic);
        for (Map.Entry<Switch, Long> entry : newCandidateSwitches.entrySet()) {
            CandidateSwitchData candidateSwitchData = candidateSwitches.get(entry.getKey());
            if (entry.getValue() < oldTraffic) {
                if (candidateSwitchData == null) {
                    candidateSwitchData = new CandidateSwitchData();
                    candidateSwitches.put(entry.getKey(), candidateSwitchData);
                }
                candidateSwitchData.newTraffic = entry.getValue();
                candidateSwitchData.active = false;
            }
        }
        active = false;
        useMyMap = true;
        onceBeenDeactivated = true;
    }

    protected void keepNewCandidateSwitchesOnly(Map<Switch, Long> newCandidateSwitches, long oldTraffic) {
        if (candidateSwitches == null) {
            candidateSwitches = new HashMap<Switch, CandidateSwitchData>();
            comparator = new SwitchComparator();
        } else {
            final Set<Switch> keySet = candidateSwitches.keySet();
            Iterator<Switch> it = keySet.iterator();
            while (it.hasNext()) {
                final Long traffic = newCandidateSwitches.get(it.next());
                if (traffic == null || traffic >= oldTraffic) {
                    it.remove();
                }
            }
        }
    }

    public boolean isUseMyMap() {
        return useMyMap;
    }

    public boolean isOnceBeenDeactivated() {
        return onceBeenDeactivated;
    }

    public void migrate(long overhead) {
        this.overhead = overhead;
        useMyMap = false;
    }

    public boolean isActive(Switch candidateSwitch) {
        return candidateSwitches.get(candidateSwitch).active;
    }

    public List<Switch> getCandidateSwitches(List<Switch> toFill) {
        toFill.clear();
        for (Map.Entry<Switch, CandidateSwitchData> entry : candidateSwitches.entrySet()) {
            if (entry.getValue().active) {
                toFill.add(entry.getKey());
            }
        }
        Collections.sort(toFill, comparator);
        return toFill;
    }

    public long getTrafficIf(Switch candidateSwitch) {
        return candidateSwitches.get(candidateSwitch).newTraffic;
    }

    public class CandidateSwitchData {
        public boolean active;
        public long newTraffic;


    }

    protected class SwitchComparator implements Comparator<Switch> {
        @Override
        public int compare(Switch o1, Switch o2) {
            final int switchesCompare = o1.compareTo(o2);
            if (switchesCompare == 0) {
                return 0;
            }
            final CandidateSwitchData data1 = candidateSwitches.get(o1);
            final CandidateSwitchData data2 = candidateSwitches.get(o2);
            if (!data1.active && data2.active) {
                //both are active
                final long trafficDiff = data1.newTraffic - data2.newTraffic;
                if (trafficDiff != 0) {
                    return trafficDiff < 0 ? -1 : 1;
                }
                return switchesCompare;
            }
            if (data1.active && !data2.active) {
                return -1;
            }
            if (!data1.active && data2.active) {
                return 1;
            }
            //if (!data1.active && !data2.active) {
            return switchesCompare;
            //}

        }
    }
}
