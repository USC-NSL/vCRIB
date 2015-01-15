package edu.usc.enl.cacheflow.algorithms.feasibility.memory.compare;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/29/12
 * Time: 12:05 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractNode {
    protected InternalNode parent;
    private int id;

    protected AbstractNode(int id) {
        this.id = id;
    }

    public abstract int getCap();

    public InternalNode getParent() {
        return parent;
    }

    public abstract int getW();

    public void setParent(InternalNode parent) {
        this.parent = parent;
    }

    public abstract int getCount();

    public abstract int getSize();

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return id+": Cap=" + getCap() +
                ", W=" + getW() +
                ", Count=" + getCount() +
                ", Size=" + getSize();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractNode that = (AbstractNode) o;

        if (id != that.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
