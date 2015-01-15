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
public class AcceptAction extends Action{
    private static AcceptAction instance;
    public static final String MATCH_STRING = "Accept";

    private AcceptAction() {
        super(MATCH_STRING);
    }

    @Override
    public Flow doAction(Flow flow) {
        return flow;
    }

    public static AcceptAction getInstance(){
        if (instance==null){
            instance=new AcceptAction();
        }
        return instance ;
    }

    @Override
    public Color getColor() {
        return Color.green;
    }
}

