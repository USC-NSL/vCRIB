package edu.usc.enl.cacheflow.algorithms.partition.decisiontree;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/18/12
 * Time: 3:44 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class TreeNodeObject {

    protected RangeDimensionRange range;
    protected int level;
    //private InternalTreeNodeObject parent = null;
    protected PartitionTree2 partitionTree2;

    TreeNodeObject(RangeDimensionRange range, int level, PartitionTree2 partitionTree2) {
        this.level = level;
        this.range = range;
        this.partitionTree2 = partitionTree2;
    }

    public boolean isLeaf() {
        final boolean leaf = this instanceof LeafTreeNodeObject;
        if (!leaf && ((InternalTreeNodeObject) this).getChildCount() == 0) {
            throw new RuntimeException("Not leaf but no child");
        }
        return leaf;
    }

    public abstract boolean hasSameShape(TreeNodeObject node2);

    protected abstract void mergeSubTrees(TreeNodeObject node2);

    public abstract boolean aggregate(TreeNodeObject other);

    /*public void update(List<DimensionRange> ranges, Set<Rule> rules) {
        this.ranges = ranges;
        this.rules = rules;
        //Collections.sort(this.rules, Rule.PRIORITY_COMPARATOR);
        updateDefinition();
    }*/

   /* public InternalTreeNodeObject getParent() {
        return parent;
    }

    protected void setParent(InternalTreeNodeObject parent) {
        this.parent = parent;
    }*/

    @Override
    public String toString() {
        return "TreeNodeObject{" +
                "level=" + level +
                ", ranges=" + range +
                '}';
    }

    public int getLevel() {
        return level;
    }


    public RangeDimensionRange getRange() {
        return range;
    }

    public Iterator<TreeNodeObject> depthFirstEnumeration() {
        return new DepthFirstIterator(this);
    }

    public abstract TreeNodeObject getMatchChildren(Long[]properties);


    /**
     * copied from Swing TreeNode implementation
     */
    public static final class DepthFirstIterator implements Iterator<TreeNodeObject> {
        protected TreeNodeObject root;
        protected Iterator<TreeNodeObject> children;
        protected Iterator<TreeNodeObject> subtree;


        public DepthFirstIterator(TreeNodeObject rootNode) {
            super();
            root = rootNode;
            if (root instanceof InternalTreeNodeObject) {
                children = ((InternalTreeNodeObject) root).getChildren();
            } else {
                children = EMPTY_ENUMERATION;
            }
            subtree = EMPTY_ENUMERATION;
        }

        public boolean hasNext() {
            return root != null;
        }

        public TreeNodeObject next() {
            TreeNodeObject retval;

            if (subtree.hasNext()) {
                retval = subtree.next();
            } else if (children.hasNext()) {
                subtree = new DepthFirstIterator(children.next());
                retval = subtree.next();
            } else {
                retval = root;
                root = null;
            }

            return retval;
        }

        public void remove() {

        }
    }

    private static final Iterator<TreeNodeObject> EMPTY_ENUMERATION = new Iterator<TreeNodeObject>() {
        public boolean hasNext() {
            return false;
        }

        public TreeNodeObject next() {
            throw new NoSuchElementException("No more elements");
        }

        public void remove() {
        }
    };

}
