package edu.usc.enl.cacheflow.algorithms.partition.decisiontree;

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
 * Time: 10:40 AM
 * To change this template use File | Settings | File Templates.
 */
class OneRuleLeafTreeNodeObjectRuleMerge extends LeafTreeNodeObject {
    private Rule rule;

    OneRuleLeafTreeNodeObjectRuleMerge(RangeDimensionRange range, Set<Rule> rules, int level, PartitionTree2 partitionTree2) {
        super(range, level, partitionTree2);
        rule = Collections.min(rules, Rule.PRIORITY_COMPARATOR);
    }

    public static Action getAction(Collection<Rule> rules) {
        return Collections.min(rules, Rule.PRIORITY_COMPARATOR).getAction();
    }

    public Rule getMinRule() {
        return rule;
    }

    public boolean aggregate(RangeDimensionRange range, Collection<Rule> applicableRules) {//sart must be the same
        if (!this.rule.equals(Collections.min(applicableRules, Rule.PRIORITY_COMPARATOR))) {
            return false;
        }
        final RangeDimensionRange newRange = getRange().canAggregate(range);
        if (newRange == null) {
            return false;
        }
        aggregate++;
        this.range = newRange;
        return true;
    }

    @Override
    public void mergeSubTrees(TreeNodeObject node2) {
    }

    public Action getAction() {
        return rule.getAction();
    }

    @Override
    public boolean aggregate(TreeNodeObject other) {
        if (!other.isLeaf()) {
            return false;
        }
        final LeafTreeNodeObject otherLeaf = (LeafTreeNodeObject) other;
        if (!this.rule.equals(otherLeaf.getMinRule())) {
            return false;
        }
        final RangeDimensionRange newRange = getRange().canAggregate(other.getRange());
        if (newRange == null) {
            return false;
        }
        aggregate++;
        range = newRange;
        return true;
    }

    @Override
    public boolean hasSameShape(TreeNodeObject child2) {
        return child2 instanceof LeafTreeNodeObject && this.rule.equals(((LeafTreeNodeObject) child2).getMinRule())
                && range.equals(child2.getRange());
    }

}
