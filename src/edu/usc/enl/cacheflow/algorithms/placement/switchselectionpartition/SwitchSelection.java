package edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 10/5/11
 * Time: 8:59 PM
 */
public abstract class SwitchSelection {
    protected Topology topology;

    public abstract <T extends List<Switch>> T sortSwitches(T toFill, Map<Partition, Switch> placement,
                                                            Partition partition);

    public void init(Topology topology) {
        this.topology = topology;
    }


    protected class ValueComparator implements Comparator<Switch> {

        Map<Switch, Double> base;

        public ValueComparator(Map<Switch, Double> base) {
            this.base = base;
        }

        public void setBase(Map<Switch, Double> base) {
            this.base = base;
        }

        public int compare(Switch a, Switch b) {
            if (base.get(a) < base.get(b)) {
                return 1;
            } else if (base.get(a).equals(base.get(b))) {

                return new Random(a.hashCode()-b.hashCode()).nextInt(3)-1;
                /* //creates more usage on edges with large capacity
                int aFreeCapacity = a.getFreeCapacity();
                int bFreeCapacity = b.getFreeCapacity();
                if (aFreeCapacity < bFreeCapacity) {
                    return 1;
                } else if (aFreeCapacity == bFreeCapacity) {
                    return a.compareTo(b);
                } else {
                    return -1;
                }*/
                //return a.compareTo(b);
            } else {
                return -1;
            }
        }
    }

    @Override
    public abstract String toString();

    protected Switch removeController(Collection<Switch> availableToChoose) {
        Switch controllerWasThere = null;
        final Iterator<Switch> iterator = availableToChoose.iterator();
        while (iterator.hasNext()) {
            Switch next = iterator.next();
            if (next.getId().equals(Switch.CONTROLLER_ID)) {
                controllerWasThere = next;
                iterator.remove();
                break;
            }
        }
        return controllerWasThere;
    }
}
