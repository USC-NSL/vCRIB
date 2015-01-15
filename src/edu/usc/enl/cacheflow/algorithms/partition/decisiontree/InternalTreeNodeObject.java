package edu.usc.enl.cacheflow.algorithms.partition.decisiontree;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;

import java.util.Iterator;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/18/12
 * Time: 3:45 PM
 * To change this template use File | Settings | File Templates.
 */
class InternalTreeNodeObject extends TreeNodeObject {
    private TreeMap<Long, TreeNodeObject> children;

    InternalTreeNodeObject(RangeDimensionRange range, int level, PartitionTree2 partitionTree2) {
        super(range, level, partitionTree2);
        children = new TreeMap<Long, TreeNodeObject>();
    }

    @Override
    public boolean hasSameShape(TreeNodeObject node2) {
        if (!(node2 instanceof InternalTreeNodeObject)) {
            return false;
        }
        if (((InternalTreeNodeObject) node2).getChildCount() != getChildCount()) {
            return false;
        }

        if (getRange().equals(node2.getRange())) {
            final Iterator<TreeNodeObject> node1Children = getChildren();
            final Iterator<TreeNodeObject> node2Children = ((InternalTreeNodeObject) node2).getChildren();
            for (int i = 0; i < getChildCount(); i++) {
                TreeNodeObject child1 = node1Children.next();
                TreeNodeObject child2 = node2Children.next();
                if (!child1.hasSameShape(child2)) {
                    return false;
                }
            }
        } else {
            return false;
        }
        return true;
    }

    @Override
    protected void mergeSubTrees(TreeNodeObject node2) {
        //need node2 to merge its children if they were leaf node

        if (!(node2 instanceof InternalTreeNodeObject)) {
            throw new RuntimeException("merge internal node with a leaf");
        }

        //merge myself
        //ranges.set(level, range);

        //merge children
        final Iterator<TreeNodeObject> node1Children = getChildren();
        final Iterator<TreeNodeObject> node2Children = ((InternalTreeNodeObject) node2).getChildren();

        for (int i = 0; i < getChildCount(); i++) {
            TreeNodeObject child1 = node1Children.next();
            TreeNodeObject child2 = node2Children.next();

            child1.mergeSubTrees(child2);
        }
    }

    public Object canAggregate(TreeNodeObject other) {
        if (!(other instanceof InternalTreeNodeObject)) {
            return null;
        }
        final InternalTreeNodeObject otherLeaf = (InternalTreeNodeObject) other;
        if (getChildCount() != otherLeaf.getChildCount()) {
            return null;
        }

        //our range should be aggregatable
        final RangeDimensionRange aggRange = getRange().canAggregate(other.getRange());
        if (aggRange == null) {
            return null;
        }
        //and our children has the same shape
        final Iterator<TreeNodeObject> node1Children = getChildren();
        final Iterator<TreeNodeObject> node2Children = otherLeaf.getChildren();
        for (int i = 0; i < getChildCount(); i++) {
            TreeNodeObject child1 = node1Children.next();
            TreeNodeObject child2 = node2Children.next();
            if (!child1.hasSameShape(child2)) {
                return null;
            }
        }
        return aggRange;
    }

    public static int aggregate = 0;

    @Override
    public boolean aggregate(TreeNodeObject other) {
        final Object aggRes = canAggregate(other);
        if (aggRes == null) {
            return false;
        }
        aggregate++;
        range = (RangeDimensionRange) aggRes;
        mergeSubTrees(other);
        return true;
    }

    public void add(TreeNodeObject child) {
        children.put(child.getRange().getStart(), child);
    }

    public TreeNodeObject getMatchChildren(Long[] properties) {
        int nextLevel = getFirst().level; //my children level may not be mylevel +1 because of one child merge in the middle of treee
        return children.floorEntry(properties[partitionTree2.permutationIndex.get(nextLevel)]).getValue();
    }

    public int getChildCount() {
        return children.size();
    }

    public Iterator<TreeNodeObject> getChildren() {
        return children.values().iterator();
    }

    protected TreeNodeObject getFirst() {
        return children.firstEntry().getValue();
    }

    public void removeAllChildren() {
        children.clear();
    }
}
