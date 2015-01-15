package edu.usc.enl.cacheflow.model;

import java.io.PrintWriter;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */
public interface WriterSerializable {
    public void toString(PrintWriter p);
    public void headerToString(PrintWriter p);
}
