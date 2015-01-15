package edu.usc.enl.cacheflow.algorithms.partition.decisiontree;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.Action;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/20/12
 * Time: 10:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class MultiRuleLeafTreeNodeObject extends LeafTreeNodeObject {
    private Set<Rule> rules;
    public static int aggregate = 0;
    private Action action;


    MultiRuleLeafTreeNodeObject(RangeDimensionRange range, Set<Rule> rules, int level, PartitionTree2 partitionTree2) {
        super(range, level, partitionTree2);
        this.rules = rules;
        Rule minRule = Collections.min(rules, Rule.PRIORITY_COMPARATOR);
        action = minRule.getAction();
    }

    public static Action getAction(Collection<Rule> rules) {
        return Collections.min(rules, Rule.PRIORITY_COMPARATOR).getAction();
    }

    public Set<Rule> getRules() {
        return rules;
    }

    public Rule getExactRule(Flow flow) {
        Rule match = null;
        for (Rule rule : rules) {
            if ((match == null || match.getPriority() >= rule.getPriority()) && rule.match(flow)) {
                match = rule;
            }
        }
        return match;
    }

    public Rule getMinRule() {
        return Collections.min(rules, Rule.PRIORITY_COMPARATOR);
    }

    public boolean aggregate(RangeDimensionRange range, Collection<Rule> applicableRules) {//sart must be the same
        if (!getAction().equals(getAction(applicableRules))) {
            return false;
        }
        final RangeDimensionRange newRange = getRange().canAggregate(range);
        if (newRange == null) {
            return false;
        }
        aggregate++;
        this.range = newRange;
        this.rules.addAll(applicableRules);
        return true;
    }

    @Override
    public void mergeSubTrees(TreeNodeObject node2) {
        if (!(node2 instanceof LeafTreeNodeObject)) {
            throw new RuntimeException("merge a leaf with internal");
        }
        this.rules.addAll(((MultiRuleLeafTreeNodeObject) node2).rules);
    }

    public Action getAction() {
        return action;
    }

    @Override
    public boolean aggregate(TreeNodeObject other) {
        if (!other.isLeaf()) {
            return false;
        }
        if (!((LeafTreeNodeObject) other).getAction().equals(getAction())) {
            return false;
        }
        final RangeDimensionRange newRange = getRange().canAggregate(other.getRange());
        if (newRange == null) {
            return false;
        }
        aggregate++;
        MultiRuleLeafTreeNodeObject otherNodeObject = ((MultiRuleLeafTreeNodeObject) other);
        range = newRange;
        rules.addAll(otherNodeObject.getRules());
        return true;
    }

}
