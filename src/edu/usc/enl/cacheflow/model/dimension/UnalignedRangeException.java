package edu.usc.enl.cacheflow.model.dimension;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/23/12
 * Time: 11:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class UnalignedRangeException extends Exception{
    public UnalignedRangeException(RangeDimensionRange range) {
        super(range+" is not aligned for wildcards");
    }
}
