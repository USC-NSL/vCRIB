package edu.usc.enl.cacheflow.algorithms.feasibility.memory.compare;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/29/12
 * Time: 12:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class VMNode extends AbstractNode {
    //private long[] pattern;
    private int originalSize;
    protected static int machineSize;

    public VMNode(int id,  int originalSize) {
        super(id);
        //this.pattern = pattern;
        this.originalSize = originalSize;
    }

    @Override
    public int getCap() {
        return parent.getCap() - parent.getW();
    }

    @Override
    /**
     * w=original-sum_parent(w)
     * cap=machine-sum_parent(w)
     * so
     * w=original-machine+cap
     */
    public int getW() {
        return originalSize - machineSize + getCap();
    }

    @Override
    public int getCount() {
        return 1;
    }

    @Override
    public int getSize() {
        return getW();
    }

    public int getOriginalSize() {
        return originalSize;
    }

    @Override
    public String toString() {
        return super.toString() +
                ", type=VM" +
                ", originalSize=" + originalSize;
    }

    /*public long[] getPattern() {
        return pattern;
    }*/
}