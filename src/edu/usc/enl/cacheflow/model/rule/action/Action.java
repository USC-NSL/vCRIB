package edu.usc.enl.cacheflow.model.rule.action;

import edu.usc.enl.cacheflow.model.Flow;

import java.awt.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 12:22 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Action {
    private String name;


    public Action(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
//        return "Action{" +
//                "name='" + name + '\'' +
//                '}';
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Action action = (Action) o;

        if (name != null ? !name.equals(action.name) : action.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public Color getColor(){
        return Color.green;
    }

    public abstract Flow doAction(Flow flow);

    public static Action getAction(String s){
        if (s.matches(DenyAction.MATCH_STRING)){
            return DenyAction.getInstance();
        }
        if (s.matches(AcceptAction.MATCH_STRING)){
            return AcceptAction.getInstance();
        }
        if (s.matches(ForwardAction.MATCH_STRING)){
            return new ForwardAction(s.replaceAll("^To\\s+",""));
        }
        System.out.println("action"+s);
        return null;
    }

}
