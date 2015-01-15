package edu.usc.enl.cacheflow.ui.exception;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/29/11
 * Time: 2:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class RuleInputException extends Exception{
    public RuleInputException(String message, Throwable cause) {
        super(message, cause);
    }

    public RuleInputException(String message) {
        super(message);
    }
}
