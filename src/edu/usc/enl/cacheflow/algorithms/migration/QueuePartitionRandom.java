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
public class QueuePartitionRandom extends QueuePartition {
    private boolean deActivateByHost;

    public QueuePartitionRandom(Partition partition, boolean active, long overhead) {
        super(partition, active, overhead);
    }

    public void deActivateBySource() {
        this.deActivateByHost = true;
    }

    public boolean isDeActivateByHost() {
        return deActivateByHost;
    }

    public void deActivate(Map<Switch, Long> newCandidateSwitches, double beta, double averagePartitionTraffic,
                           double chanceRatio, long oldTraffic, Switch currentSwitch) {
        keepNewCandidateSwitchesOnly(newCandidateSwitches);
        for (Map.Entry<Switch, Long> entry : newCandidateSwitches.entrySet()) {
            if (!entry.getKey().equals(currentSwitch) &&
                    RandomMinTrafficSwitchSelection.convertTrafficToRate(beta, oldTraffic - entry.getValue(), averagePartitionTraffic) >= chanceRatio) {
                CandidateSwitchData candidateSwitchData = candidateSwitches.get(entry.getKey());
                if (candidateSwitchData == null) {
                    candidateSwitchData = new CandidateSwitchData();
                    candidateSwitches.put(entry.getKey(), candidateSwitchData);
                }
                candidateSwitchData.newTraffic = entry.getValue();
                candidateSwitchData.active = false;
            } else {
                candidateSwitches.remove(entry.getKey());
            }
        }
        active = false;
        useMyMap = true;
        onceBeenDeactivated = true;
        if (deActivateByHost) {
            //so newCandidateSwitches must be empty
            if (newCandidateSwitches.size() > 0) {
                throw new RuntimeException("Deactivate by isSource and not use my map but have candidate switch?");
            }
            useMyMap = false;
        }
    }

    protected void keepNewCandidateSwitchesOnly(Map<Switch, Long> newCandidateSwitches) {
        if (candidateSwitches == null) {
            candidateSwitches = new HashMap<Switch, CandidateSwitchData>();
            comparator = new SwitchComparator();
        } else {
            candidateSwitches.keySet().retainAll(newCandidateSwitches.keySet());
        }
    }

    @Override
    public boolean isUseMyMap() {
        return super.isUseMyMap() && !deActivateByHost;
    }

    @Override
    public boolean isActive() {
        return super.isActive() && !deActivateByHost;
    }

    public void notDeActivateByHost() {
        deActivateByHost = false;
    }

    public void activateByHost() {
        deActivateByHost = false;

    }
}
