package edu.usc.enl.cacheflow.model.rule;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.WriterSerializable;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.ui.DrawableRuleSpace;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.io.PrintWriter;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 12:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class Rule implements DrawableRuleSpace, WriterSerializable {
    public static Comparator<Rule> PRIORITY_COMPARATOR = getPriorityComparator();
    public static Comparator<Rule> ID_COMPARATOR = new Comparator<Rule>() {
        public int compare(Rule o1, Rule o2) {
            return o1.id - o2.id;
        }
    };

    private Action action;

    private final List<RangeDimensionRange> properties;
    private int priority;
    private long wildcardCache = -1;
    private int id = 0;
    public static int maxId = 0;

    public static void resetMaxId() {
        maxId = 0;
    }

    public Rule(Action action, List<RangeDimensionRange> properties, int priority, int id) {
        setId(id);
        this.action = action;
        this.properties = properties;
        this.priority = priority;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
        maxId = maxId < id ? id : maxId;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    // does not break rules
    public static Map<RangeDimensionRange, ? extends Collection<Rule>> categorizeRuleSpace(Collection<Rule> rules, List<RangeDimensionRange> ranges, int dimensionIndex) {
        Map<RangeDimensionRange, List<Rule>> output = new HashMap<RangeDimensionRange, List<Rule>>();
        for (RangeDimensionRange range : ranges) {
            List<Rule> rangeRules = new LinkedList<Rule>();
            output.put(range, rangeRules);
            for (Rule rule : rules) {
                if (rule.getProperty(dimensionIndex).hasIntersect(range)) {
                    rangeRules.add(rule);
                }
            }
        }
        return output;
    }

    public static Map<RangeDimensionRange, Collection<Rule>> partitionRuleSpace(Collection<Rule> rules, List<RangeDimensionRange> ranges, int dimensionIndex) {
        List<Rule> newRules = new LinkedList<Rule>(rules);
        Map<RangeDimensionRange, Collection<Rule>> outputMap = new TreeMap<RangeDimensionRange, Collection<Rule>>();
        for (RangeDimensionRange range : ranges) {
            //find rules that match this range
            List<Rule> thisRangeRules = new LinkedList<Rule>();
            outputMap.put(range, thisRangeRules);
            List<Rule> nextRangeRules = new LinkedList<Rule>();
            for (Rule rule : newRules) {
                RangeDimensionRange intersection = rule.getProperty(dimensionIndex).intersect(range);
                if (intersection == null) {
                    nextRangeRules.add(rule); //keep this rule for other ranges
                }
                if (intersection != null) {
                    {
                        //add intersection to this range rules
                        List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(rule.getProperties());
                        properties.set(dimensionIndex, intersection);
                        Rule newRule = new Rule(rule.getAction(), properties, rule.getPriority(), maxId + 1);
                        thisRangeRules.add(newRule);
                    }
                    //find the other parts and add it to the nextRangeRules
                    List<RangeDimensionRange> minusResult = rule.getProperty(dimensionIndex).minus(intersection);
                    if (minusResult.size() == 1 && minusResult.equals(rule.getProperty(dimensionIndex))) {//no intersection
                        // does not reach here because it has been checked before
                        // but if reachs:
                        nextRangeRules.add(rule); //keep this rule for other ranges
                        System.out.println("should not reach here!!");
                    } else if (minusResult.size() == 0) {//no need to break
                        //we have added the intersection before
                        //thisRangeRules.add(rule);
                    } else {//break the rule
                        for (RangeDimensionRange minusRange : minusResult) { //if ranges are sorted should not be need for 2 breaks
                            List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(rule.getProperties());
                            properties.set(dimensionIndex, minusRange);
                            Rule newRule = new Rule(rule.getAction(), properties, rule.getPriority(), maxId + 1);
                            nextRangeRules.add(newRule);
                        }
                    }
                }
            }
            newRules = nextRangeRules;
        }
        return outputMap;
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        Rule other = (Rule) obj;
        if (id != 0 && other.id != 0) {
            return id == other.id;
        }
        return getAction().equals(other.getAction()) && getPriority() == other.getPriority() &&
                equalProperties(other);
    }

    @Override
    public int hashCode() {
        return id;
    }


    public RangeDimensionRange getProperty(int i) {
        return properties.get(i);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(action.toString());
        sb.append(",").append(id).append(" = {");
        for (RangeDimensionRange property : properties) {
            //sb.append(propertyName).append(":").append(properties.get(propertyName)).append(" ");
            sb.append(property).append(", ");
        }
        sb.append("}, ").append(priority);
        return sb.toString();
    }

    public boolean match(Flow flow) {
        for (int i = 0; i < properties.size(); i++) {
            if (!properties.get(i).match(flow.getProperty(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean match(long[] flowProperties) {
        for (int i = 0; i < properties.size(); i++) {
            if (!properties.get(i).match(flowProperties[i])) {
                return false;
            }
        }
        return true;
    }

    public Action getAction() {
        return action;
    }

    /**
     * TODO: this should be completely optimized
     *
     * @param rule
     * @return
     */
    public Rule canAggregate(Rule rule) {
        if (!action.equals(rule.getAction())) {
            return null;
        }

        /* No need to do this

        boolean intersectExist = true;
        //check inclusion
        Map<DimensionInfo, RangeDimensionRange> intersectProperties = new HashMap<DimensionInfo, RangeDimensionRange>();
        //find inclusion
        for (DimensionInfo propertyName : properties.keySet()) {
            RangeDimensionRange property1 = properties.get(propertyName);
            RangeDimensionRange property2 = rule.getProperty(propertyName);
            RangeDimensionRange intersect = property1.intersect(property2);
            if (intersect == null) {
                intersectExist = false;
                break;
            }
            intersectProperties.put(propertyName, intersect);
        }

        if (intersectExist) {
            //check inclusion
            if (isSimilar(intersectProperties, properties)) {
                return rule;
            } else if (isSimilar(intersectProperties, rule.getProperties())) {
                return this;
            }
        }*/

        //check dimensions only one dimension can be different

        List<RangeDimensionRange> aggregateProperties = new ArrayList<RangeDimensionRange>(properties);//just fill it although it is not necessary
        for (int i = 0; i < properties.size(); i++) {

            RangeDimensionRange myProperty = properties.get(i);
            RangeDimensionRange otherRuleProperty = rule.getProperty(i);
            RangeDimensionRange myNewDimensionRange = myProperty.canAggregate(otherRuleProperty);
            if (myNewDimensionRange == null) {
                return null;
            } else {
                aggregateProperties.set(i, myNewDimensionRange);
            }
        }
        boolean foundDiffDimension = false;
        for (int i = 0; i < aggregateProperties.size(); i++) {
            RangeDimensionRange value = aggregateProperties.get(i);
            if (!(properties.get(i).equals(value) && rule.getProperties().get(i).equals(value))) {
                if (foundDiffDimension) {
                    return null;//spurious aggregation
                } else {
                    foundDiffDimension = true;
                }
            }
        }
        return new Rule(action, aggregateProperties, Math.min(priority, rule.getPriority()), maxId + 1);
    }

    private boolean isSimilar(Map<DimensionInfo, RangeDimensionRange> ranges1, Map<DimensionInfo, RangeDimensionRange> ranges2) {
        for (Map.Entry<DimensionInfo, RangeDimensionRange> entry : ranges1.entrySet()) {
            if (!ranges2.get(entry.getKey()).equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }


    public List<RangeDimensionRange> getProperties() {
        return properties;
    }

    public void paint(Graphics2D g2, int d1, int d2, double zoomCoefficient) {
        Color oldColor = g2.getColor();
        g2.setColor(action.getColor());
        int x1 = (int) (zoomCoefficient * getProperty(d1).getStart());
        int x2 = (int) (zoomCoefficient * getProperty(d1).getEnd());
        int y1 = (int) (zoomCoefficient * getProperty(d2).getStart());
        int y2 = (int) (zoomCoefficient * getProperty(d2).getEnd());
        g2.fillRect(x1, y1, x2 - x1, y2 - y1);
        g2.setColor(Color.black);
        g2.drawRect(x1, y1, x2 - x1, y2 - y1);
        g2.setColor(Color.WHITE);
        g2.scale(zoomCoefficient, zoomCoefficient);
        //g2.drawString(action.toString(), (int) (1 / zoomCoefficient * x1), (int) (1 / zoomCoefficient * y1 + 10));
        g2.scale(1 / zoomCoefficient, 1 / zoomCoefficient);
        /* System.out.println(x1 + " " +
 y1 + " " +
 (x2 - x1) + " " +
 (y2 - y1) + " " + action.getColor());*/
        g2.setColor(oldColor);
    }

    protected static Comparator<Rule> getPriorityComparator() {
        return new Comparator<Rule>() {
            public int compare(Rule o1, Rule o2) {
                return o1.getPriority() - o2.getPriority();
            }
        };
    }

    public boolean equalProperties(Rule rule) {
        for (int i = 0; i < properties.size(); i++) {
            if (!properties.get(i).equals(rule.getProperty(i))) {
                return false;
            }
        }
        return true;
    }

    public static JPanel createCanvas(final List<Rule> rules, final int d1, final int d2) {
        return new JPanel() {

            @Override
            public void paintComponent(Graphics g) {
                super.paintComponent(g);
                System.out.println("=======================");
                Graphics2D g2 = (Graphics2D) g;
                g2.setTransform(new AffineTransform(0.5, 0, 0, 0.5, 0, 0));
                for (Rule rule : rules) {
                    rule.paint(g2, d1, d2, 1);
                }
            }
        };
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public void updateProperties(){
        wildcardCache=-1;
    }

    public long getWildCardBitPattern() throws UnalignedRangeException {
        if (wildcardCache == -1) {
            wildcardCache = RangeDimensionRange.computeWildcardPattern(properties);
        }
        return wildcardCache;
    }

    public void toString(PrintWriter p) {
        RangeDimensionRange.toString(p, properties);
        p.println("," + action + "," + priority + "," + id);
    }

    public void headerToString(PrintWriter writer) {
        DimensionInfo.dimensionInfosToString(Util.getDimensionInfos(), writer);
    }
}
