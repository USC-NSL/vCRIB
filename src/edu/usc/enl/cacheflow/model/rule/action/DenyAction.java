package edu.usc.enl.cacheflow.model.rule.action;

import edu.usc.enl.cacheflow.model.Flow;

import java.awt.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/17/11
 * Time: 12:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class DenyAction extends Action{
    private static DenyAction instance;
    public static final String MATCH_STRING ="Deny";

    private DenyAction() {
        super(MATCH_STRING);
    }

    @Override
    public Flow doAction(Flow flow) {
        return null;
    }

    public static DenyAction getInstance(){
        if (instance==null){
            instance= new DenyAction();
        }
        return instance;
    }

    @Override
    public Color getColor() {
        return Color.red;
    }
}
