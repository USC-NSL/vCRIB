package edu.usc.enl.cacheflow.ui.exception;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/29/11
 * Time: 12:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class FileException extends Exception {
    public FileException(String message, Throwable cause, File file) {
        super(message + " in file " + file.getPath(), cause);
    }
}
