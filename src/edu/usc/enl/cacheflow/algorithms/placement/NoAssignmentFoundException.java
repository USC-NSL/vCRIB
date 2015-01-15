package edu.usc.enl.cacheflow.algorithms.placement;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/12/11
 * Time: 1:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class NoAssignmentFoundException extends Exception{
    public NoAssignmentFoundException() {
        this("No solution for assignment found");
    }

    public NoAssignmentFoundException(String message) {
        super(message);
    }

    public NoAssignmentFoundException(Throwable cause) {
        super(cause);
    }
}
