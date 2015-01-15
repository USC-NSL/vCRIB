package edu.usc.enl.cacheflow.processor.partition.transform;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/30/12
 * Time: 6:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class IncompleteTransformException extends Exception{
    public IncompleteTransformException(String message) {
        super(message);
    }

    public IncompleteTransformException(String message, Throwable cause) {
        super(message, cause);
    }
}
