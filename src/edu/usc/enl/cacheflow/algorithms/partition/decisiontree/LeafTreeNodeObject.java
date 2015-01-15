package edu.usc.enl.cacheflow.algorithms.partition.decisiontree;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.Action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/18/12
 * Time: 3:45 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class  LeafTreeNodeObject extends TreeNodeObject {
    public static int aggregate = 0;

    LeafTreeNodeObject(RangeDimensionRange range, int level, PartitionTree2 partitionTree2) {
        super(range, level, partitionTree2);
    }

    public static Action getAction(Collection<Rule> rules) {
        return Collections.min(rules, Rule.PRIORITY_COMPARATOR).getAction();
    }

    public abstract Rule getMinRule();

    @Override
    public TreeNodeObject getMatchChildren(Long[] properties) {
        return this;
    }

    @Override
    public boolean hasSameShape(TreeNodeObject child2) {
        if (!(child2 instanceof LeafTreeNodeObject)) {
            return false;
        }
        final Action action2 = ((LeafTreeNodeObject) child2).getAction();
        return getAction().equals(action2) &&
                getRange().equals(child2.getRange());
    }

    public abstract boolean aggregate(RangeDimensionRange range, Collection<Rule> rules);

    public abstract Action getAction() ;

    @Override
    public String toString() {
        return "LeafTreeNodeObject{" +
                "action=" + getAction() +
                ", ranges=" + range +
                '}';
    }

    public Rule generateRule() {
        List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(partitionTree2.permutation.size());
        /*int treeLevel = 0;
        for (RangeDimensionRange range : ranges) {
            properties.set(partitionTree2.permutationIndex.get(treeLevel), range);
            treeLevel++;
        }*/
        return new Rule(getAction(), properties, 0,Rule.maxId+1);
    }
}
