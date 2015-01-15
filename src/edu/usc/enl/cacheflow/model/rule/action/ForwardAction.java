package edu.usc.enl.cacheflow.model.rule.action;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.awt.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/17/11
 * Time: 1:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class ForwardAction extends Action {
    private String destinationID;
    public static final String MATCH_STRING = "^To\\s+\\S+";

    public ForwardAction(Switch destination) {
        this(destination.getId());
    }

    public ForwardAction(String destination) {
        super("To " + destination);
        this.destinationID = destination;
    }

    public String getDestination() {
        return destinationID;
    }

    @Override
    public Flow doAction(Flow flow) {
        return flow;
    }

    @Override
    public boolean equals(Object o) {
        boolean superEqual = super.equals(o);
        return superEqual && ((ForwardAction) o).destinationID.equals(this.destinationID);
    }

    public void setDestinationID(String destinationID) {
        this.destinationID = destinationID;
    }

    @Override
    public Color getColor() {
        return Color.BLUE;
    }
}
