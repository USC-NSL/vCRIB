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
class OneRuleLeafTreeNodeObject extends LeafTreeNodeObject {
    private Rule rule;

    OneRuleLeafTreeNodeObject(RangeDimensionRange range, Set<Rule> rules, int level, PartitionTree2 partitionTree2) {
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
        if (!getAction().equals(getAction(applicableRules))) {
            return false;
        }
        final RangeDimensionRange newRange = getRange().canAggregate(range);
        if (newRange == null) {
            return false;
        }
        aggregate++;
        this.range = getRange().canAggregate(range);
        final Rule otherRule = Collections.min(applicableRules, Rule.PRIORITY_COMPARATOR);
        this.rule = rule.getPriority() < otherRule.getPriority() ? this.rule : otherRule;
        return true;
    }

    @Override
    public void mergeSubTrees(TreeNodeObject node2) {
        final Rule otherRule = ((LeafTreeNodeObject) node2).getMinRule();
        this.rule = rule.getPriority() < otherRule.getPriority() ? this.rule : otherRule;
    }

    public Action getAction() {
        return rule.getAction();
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
        range = newRange;
        LeafTreeNodeObject otherNodeObject = ((LeafTreeNodeObject) other);
        final Rule otherRule = otherNodeObject.getMinRule();
        this.rule = rule.getPriority() < otherRule.getPriority() ? this.rule : otherRule;
        return true;
    }
}
