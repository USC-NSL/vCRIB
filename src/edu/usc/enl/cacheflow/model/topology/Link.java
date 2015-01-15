package edu.usc.enl.cacheflow.model.topology;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.HasStatistics;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.WriterSerializable;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 12:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class Link implements HasStatistics, WriterSerializable {
    private Switch start;
    private Switch end;
    private final long capacity;
    private long usedCapacity;
    private final int delay;
    private List<Flow> flows;
    private Link otherSide;

    public static final String USED_CAPACITY_STAT = "Used Capacity";
    public static final String STRESS_STAT = "Stress";

    public Link(Switch start, Switch end, long capacity) {
        this(start, end, capacity, 1);
    }

    public Link(Switch start, Switch end, long capacity, int delay) {
        this.start = start;
        this.end = end;
        this.capacity = capacity;
        this.delay = delay;
        flows = new LinkedList<Flow>();
    }

    public int getDelay() {
        return delay;
    }

    public List<Flow> getFlows() {
        return flows;
    }

    public void setFlows(List<Flow> flows) {
        this.flows = flows;
    }

    public void addFlow(Flow flow) {
        flows.add(flow);
    }

    public Link getOtherSide() {
        return otherSide;
    }

    public void setOtherSide(Link otherSide) {
        this.otherSide = otherSide;
    }

    public double getDelayTrafficProduct() {
        return usedCapacity * delay;
    }

    public Switch getStart() {
        return start;
    }

    public Switch getEnd() {
        return end;
    }

    public void setUsedCapacity(long usedCapacity) {
        this.usedCapacity = usedCapacity;
    }


    public long getUsedCapacity() {
        return usedCapacity;
    }

    @Override
    public String toString() {
        return "Link{" +
                "start=" + start.getId() +
                ", end=" + end.getId() +
                ", capacity=" + capacity +
                '}';
    }

    public long getCapacity() {
        return capacity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Link link = (Link) o;

        if (end != null ? !end.equals(link.end) : link.end != null) return false;
        if (start != null ? !start.equals(link.start) : link.start != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = start != null ? start.hashCode() : 0;
        result = 31 * result + (end != null ? end.hashCode() : 0);
        return result;
    }

    public Statistics getStats() {
        Statistics statistics = new Statistics();
        statistics.addStat(USED_CAPACITY_STAT, getUsedCapacity());
        statistics.addStat(STRESS_STAT, 1.0 * getUsedCapacity() / getCapacity());
        return statistics;
    }

    public void reset() {
        usedCapacity = 0;
        flows.clear();
    }

    public void toString(PrintWriter p) {
        p.println(start + "," + end + "," + capacity);
    }

    public void headerToString(PrintWriter p) {
    }
}
