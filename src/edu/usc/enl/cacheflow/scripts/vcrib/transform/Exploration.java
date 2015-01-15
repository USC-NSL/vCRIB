package edu.usc.enl.cacheflow.scripts.vcrib.transform;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.processor.partition.transform.IncompleteTransformException;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 11/1/12
 * Time: 12:02 PM
 * To change this template use File | Settings | File Templates.
 */
 abstract class Exploration {
    public static enum Action {NULL, EXTEND, BREAK, ADD, REMOVE}
    public abstract int explore(int tryNum, boolean feasible) throws NoAssignmentFoundException, IncompleteTransformException;
}
