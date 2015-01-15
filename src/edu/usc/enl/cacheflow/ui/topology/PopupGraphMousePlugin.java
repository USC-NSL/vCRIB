package edu.usc.enl.cacheflow.ui.topology;

import edu.uci.ics.jung.algorithms.layout.GraphElementAccessor;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.AbstractPopupGraphMousePlugin;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.geom.Point2D;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/12/11
 * Time: 11:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class PopupGraphMousePlugin extends AbstractPopupGraphMousePlugin
    	implements MouseListener {
    @Override
    protected void handlePopup(MouseEvent e) {
        final VisualizationViewer<Switch,Link> vv =
                (VisualizationViewer<Switch,Link>)e.getSource();
            Point2D p = e.getPoint();//vv.getRenderContext().getBasicTransformer().inverseViewTransform(e.getPoint());

            GraphElementAccessor<Switch,Link> pickSupport = vv.getPickSupport();
            if(pickSupport != null) {
                final Switch aSwitch = pickSupport.getVertex(vv.getGraphLayout(), p.getX(), p.getY());
                if(aSwitch != null) {
                    JPopupMenu popup = new JPopupMenu();
                    popup.add(new AbstractAction("Properties") {
                        public void actionPerformed(ActionEvent e) {
                        	new SwitchPropertiesWindow(aSwitch).setVisible(true);
                        }
                    });
                    popup.show(vv, e.getX(), e.getY());
                } else {
                    final Link link = pickSupport.getEdge(vv.getGraphLayout(), p.getX(), p.getY());
                    if(link != null) {
                        JPopupMenu popup = new JPopupMenu();
                        popup.add(new AbstractAction("Properties") {
                            public void actionPerformed(ActionEvent e) {
                                new LinkPropertiesWindow(link).setVisible(true);
                            }
                        });
                        popup.show(vv, e.getX(), e.getY());

                    }
                }
            }
    }
}
