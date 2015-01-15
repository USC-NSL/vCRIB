package edu.usc.enl.cacheflow.model.rule;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.HasStatistics;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.WriterSerializable;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.exceptions.NoMatchRuleException;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.rule.action.ForwardAction;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.statistics.RulesStatisticsProcessor;
import edu.usc.enl.cacheflow.ui.DrawableRuleSpace;
import edu.usc.enl.cacheflow.util.Util;

import java.awt.*;
import java.io.PrintWriter;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 10/5/11
 * Time: 4:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class Partition implements Comparable<Partition>, DrawableRuleSpace, HasStatistics, WriterSerializable {
    private List<RangeDimensionRange> properties;
    private Collection<Rule> rules;

    public static final String NUMBER_OF_RULES_STAT = "Number of Rules";
    public static final String WILDCARD_SETS_STAT = "Wildcard Sets";
    private int hashCodeCache = 0;
    private int id = 0;

    public static Comparator<Partition> ID_COMPARATOR = new Comparator<Partition>() {
        public int compare(Partition o1, Partition o2) {
            return o1.id - o2.id;
        }
    };

    public static Comparator<Partition> getPropertyComparator(final int propertyIndex){
        return new Comparator<Partition>() {
            @Override
            public int compare(Partition o1, Partition o2) {
                return o1.getProperty(propertyIndex).compareTo(o2.getProperty(propertyIndex));
            }
        };
    }


    public Partition(Collection<Rule> rules, List<RangeDimensionRange> properties) {
        this.rules = rules;
        this.properties = properties;
    }

    public Partition(Collection<Rule> rules) {
        properties = new ArrayList<RangeDimensionRange>();
        this.rules = rules;
        //Collections.sort(this.rules, Rule.PRIORITY_COMPARATOR);
        if (rules.size() > 0) {
            Iterator<Rule> iterator;

            for (int dim = 0; dim < Util.getDimensionInfos().size(); dim++) {
                iterator = rules.iterator();
                RangeDimensionRange dimensionRange = iterator.next().getProperty(dim);
                long start = dimensionRange.getStart();
                long end = dimensionRange.getEnd();
                while (iterator.hasNext()) {
                    Rule rule = iterator.next();
                    RangeDimensionRange property = rule.getProperty(dim);
                    start = Math.min(start, property.getStart());
                    end = Math.max(end, property.getEnd());
                }
                properties.add(new RangeDimensionRange(start, end, dimensionRange.getInfo()));
            }
        }
    }

    public void setRules(Collection<Rule> rules) {
        this.rules = rules;
    }

    public Collection<Rule> getRules() {
        return rules;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append("{");
        for (RangeDimensionRange property : properties) {
            //sb.append(propertyName).append(":").append(properties.get(propertyName)).append(" ");
            sb.append(property).append(", ");
        }
        sb.append("}");
        return sb.toString();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
        hashCodeCache=0;
    }

    public int getSize() {
        return rules.size();
    }

    public List<RangeDimensionRange> getProperties() {
        return properties;
    }

    public RangeDimensionRange getProperty(int i) {
        return properties.get(i);
    }

    public int compareTo(Partition partition) {
        for (int i = 0; i < properties.size(); i++) {
            RangeDimensionRange myProperty = properties.get(i);
            RangeDimensionRange otherProperty = partition.getProperty(i);
            int c = myProperty.compareTo(otherProperty);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    public void paint2(Graphics2D gPartition, Graphics2D gRules, int d1, int d2, double zoomCoefficient) {
        for (Rule rule : rules) {
            rule.paint(gRules, d1, d2, zoomCoefficient);
        }

        Color oldColor = gPartition.getColor();
        int x1 = (int) (zoomCoefficient * getProperty(d1).getStart());
        int x2 = (int) (zoomCoefficient * getProperty(d1).getEnd());
        int y1 = (int) (zoomCoefficient * getProperty(d2).getStart());
        int y2 = (int) (zoomCoefficient * getProperty(d2).getEnd());
        gPartition.setColor(Color.black);
        gPartition.drawRect(x1, y1, x2 - x1, y2 - y1);
        System.out.println(x1 + " " +
                y1 + " " +
                (x2 - x1) + " " +
                (y2 - y1) + " ");
        gPartition.setColor(oldColor);

    }

    public void paint(Graphics2D g2, int d1, int d2, double zoomCoefficient) {
        Color oldColor = g2.getColor();
        int x1 = (int) (zoomCoefficient * getProperty(d1).getStart());
        int x2 = (int) (zoomCoefficient * getProperty(d1).getEnd());
        int y1 = (int) (zoomCoefficient * getProperty(d2).getStart());
        int y2 = (int) (zoomCoefficient * getProperty(d2).getEnd());
        g2.setColor(Color.black);
        g2.drawRect(x1, y1, x2 - x1, y2 - y1);
        /*System.out.println(x1 + " " +
                y1 + " " +
                (x2 - x1) + " " +
                (y2 - y1) + " ");*/
        g2.setColor(oldColor);
    }

    public Statistics getStats() {
        Statistics stat = new Statistics();
        stat.addStat(NUMBER_OF_RULES_STAT, rules.size());
        stat.addStat(WILDCARD_SETS_STAT, RulesStatisticsProcessor.getNumberOfUniqueWildcardSets2(rules));
        return stat;
    }

    @Override
    public int hashCode() {
        if (hashCodeCache != 0) {//for performance reason to not create hashmap iterator!
            return hashCodeCache;
        } else {
            if (id != 0) {
                hashCodeCache = id;
                return hashCodeCache;
            }
            int hash = 1;
            for (RangeDimensionRange dimensionRange : properties) {
                hash = hash * 31 + dimensionRange.hashCode();
            }
            hashCodeCache = hash;
            return hash;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        final Partition other = (Partition) obj;
        if (id != 0 && other.id != 0) {
            return id == other.id;
        }
        final List<RangeDimensionRange> properties1 = other.getProperties();
        for (int i = 0, propertiesSize = properties.size(); i < propertiesSize; i++) {
            if (!properties.get(i).equals(properties1.get(i))) {
                return false;
            }
        }
        /*for (int i = 0, rulesSize = rules.size(); i < rulesSize; i++) {
            Rule rule = rules.get(i);
            obj1.rules.get(i);
        }*/
        return true;
    }

   /* public Rule getForwardingRule(Switch s) {
        return getForwardingRule(s, FORWARDING_RULES_PRIORITY);
    }

    public Rule getForwardingRule(Switch s, int priority) {
        return getForwardingRule(new ForwardAction(s), priority);
    }

    public Rule getForwardingRule(Action a, int priority) {
        return new Rule(a, getProperties(), priority);
    }*/

    public long getWildcardPattern() throws UnalignedRangeException {
        return RangeDimensionRange.computeWildcardPattern(properties);
    }

    public void toString(PrintWriter p) {
        RangeDimensionRange.toString(p, properties);
        p.println();
        for (Rule rule : rules) {
            rule.toString(p);
        }
    }

    public void headerToString(PrintWriter p) {
        DimensionInfo.dimensionInfosToString(Util.getDimensionInfos(), p);
    }


    public static class SizeComparator implements Comparator<Partition> {

        public int compare(Partition o1, Partition o2) {
            return o1.getSize() - o2.getSize();
        }
    }
}
