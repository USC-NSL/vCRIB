package edu.usc.enl.cacheflow.algorithms.feasibility.memory.compare;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/29/12
 * Time: 12:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class InternalNode extends AbstractNode {
    private int count;
    private int size;
    private AbstractNode left;
    private AbstractNode right;
    private int w;
    private int cap;

    public InternalNode(int id, int w, AbstractNode left, AbstractNode right) {
        super(id);
        this.w = w;
        this.left = left;
        this.right = right;
        left.setParent(this);
        right.setParent(this);
    }

    @Override
    public int getCap() {
        return cap;
    }

    public void setCap(int cap) {
        this.cap = cap;
        if ((cap - w) < 0 || (cap == w && left.getW() + right.getW() > 0)) {
            throw new RuntimeException("Negative capacity for my children");
        }
    }

    @Override
    public int getW() {
        return w;
    }

    @Override
    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public int getSize() {
        return size;
    }

    public AbstractNode getLeft() {
        return left;
    }

    public AbstractNode getRight() {
        return right;
    }

    public void computeCountSize() {
        int childSum = left.getSize() + right.getSize();
        /*if (childSum == 0) {
            setCount(1);
        }else if (cap==w) {//childsum is not zero but my capacity for them is zero!
            throw new RuntimeException("Negative capacity for my children"); //this case is checked in setcap before
        } else {
            setCount((int) Math.ceil(1.0 * childSum / (cap-w)));
        }*/

        setCount(Math.max(1, (int) Math.ceil(1.0 * childSum / (cap - w))));
        setSize(childSum + getCount() * getW());
    }

    @Override
    public String toString() {
        return super.toString() +
                ", type=Internal";
    }

    public InternalNode remove() throws LastSingleNode {
        //update ancestors and add them to the queue if eligible
        AbstractNode sibling = parent.getLeft().equals(this) ? parent.getRight() : parent.getLeft();
        //note that sibling can be vmnode so let's replace parent with sibling
        if (parent.parent != null) {
            if (parent.parent.left.equals(parent)) {
                parent.parent.left = sibling;
            } else {
                parent.parent.right = sibling;
            }
        }
        //update sibling data
        sibling.parent = parent.parent;
        if (sibling instanceof InternalNode) {
            InternalNode siblingI = (InternalNode) sibling;
            siblingI.w = sibling.getW() + parent.getW();
            siblingI.cap = parent.getCap();
            siblingI.setSize(siblingI.getSize() + parent.getW() * sibling.getCount());
        }
        if (sibling.parent == null && sibling.getCount() == 1) {
            throw new LastSingleNode(sibling);
        }
        //now update parents
        InternalNode curI = sibling.parent;
        InternalNode newCandidate = null;
        while (curI != null) {
            //update cur
            curI.computeCountSize();
            if (curI.getCount() == 2 &&
                    curI.getLeft().getCount() == 1 && curI.getRight().getCount() == 1) {//can only enter here once as if once got in
                //its parent cannot get in and always parent.count>=count
                newCandidate = curI;
            }
            if (curI.parent == null && curI.getCount() == 1) {//I am a single root
                throw new LastSingleNode(curI);
            }
            curI = curI.parent;
        }
        return newCandidate;
    }

    public class LastSingleNode extends Exception {
        private AbstractNode rootNode;

        public LastSingleNode(AbstractNode rootNode) {
            this.rootNode = rootNode;
        }

        public AbstractNode getRootNode() {
            return rootNode;
        }
    }

}