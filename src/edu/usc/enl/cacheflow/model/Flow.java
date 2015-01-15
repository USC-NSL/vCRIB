package edu.usc.enl.cacheflow.model;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.Comparator;
import java.util.SortedMap;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 12:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class Flow implements WriterSerializable {
    private Long[] properties;
    private Switch source;
    private Switch destination;
    private int hashCode = 0;

    private long traffic;
    private boolean isNew = true;

    public Flow(long traffic, Switch source, Switch destination, SortedMap<DimensionInfo, Long> properties) {
        this.traffic = traffic;
        this.source = source;
        this.destination = destination;
        this.properties = new Long[properties.size()];
        int i = 0;
        for (Long aLong : properties.values()) {
            this.properties[i++] = aLong;
        }

    }

    public Flow(long traffic, Switch source, Switch destination, Long[] properties) {
        this.traffic = traffic;
        this.source = source;
        this.destination = destination;
        this.properties = properties;
    }

    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean aNew) {
        isNew = aNew;
    }

    public Switch getSource() {
        return source;
    }

    public Switch getDestination() {
        return destination;
    }

    public void setSource(Switch source) {
        this.source = source;
    }

    public void setTraffic(long traffic) {
        this.traffic = traffic;
    }

    public void setDestination(Switch destination) {
        this.destination = destination;
    }

    public Long getProperty(int i) {
        return properties[i];
    }

    public long getTraffic() {
        return traffic;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(traffic + " " + source + "->" + destination + " : {");
        for (Long property : properties) {
            sb.append(property).append(", ");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        if (hashCode != 0) {//for performance reason to not create hashmap iterator!
            return hashCode;
        } else {
            int hash = 1;
            for (Long property : properties) {
                hash = hash * 31 + property.hashCode();
            }
//            //TODO it should not be necessary but because of lack of IP assignment it is here
//            hash = hash * 31 + source.hashCode();
//            hash = hash * 31 + destination.hashCode();
            hashCode = hash;
            return hash;
        }
    }

    private Flow(int length) {
        properties = new Long[length];
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        Flow f = (Flow) obj;
        if (!(source.equals(f.getSource()) && destination.equals(f.getDestination()) &&
                traffic == f.getTraffic()) && isNew() == f.isNew()) {
            return false;
        }
        for (int i = 0; i < properties.length; i++) {
            if (!getProperty(i).equals(f.getProperty(i))) {
                return false;
            }
        }
        return true;
    }

    public Flow duplicate() {
        Flow flow = new Flow(properties.length);
        flow.setSource(getSource());
        flow.setDestination(getDestination());
        flow.setTraffic(getTraffic());
        flow.setNew(isNew);
        int i = 0;
        for (Long property : properties) {
            flow.properties[i++] = property;
        }
        return flow;
    }


    public Long[] getProperties() {
        return properties;
    }

    public void setProperty(int i, long l) {
        properties[i] = l;
        hashCode = 0;
    }

    public static Comparator<Flow> getFieldComparator(final int propertyIndex) {
        return new Comparator<Flow>() {
            public int compare(Flow o1, Flow o2) {
                return o1.getProperty(propertyIndex).compareTo(o2.getProperty(propertyIndex));
            }
        };
    }

    public void toString(PrintWriter p) {
        boolean first = true;
        for (Long property : properties) {
            if (!first) {
                p.print(",");
            }
            first = false;
            p.print(property);
        }
        p.println("," + source.getId() + "," + destination.getId() + "," + traffic + "," + isNew);
    }

    public void headerToString(PrintWriter p) {
        DimensionInfo.dimensionInfosToString(Util.getDimensionInfos(), p);
    }
}
