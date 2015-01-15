package edu.usc.enl.cacheflow.ui.rulerender;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/29/11
 * Time: 11:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class RenderWindow extends JFrame {
    public static final Dimension PREFERREDSIZE = new Dimension(300, 300);

    private RenderWindow thisFrame;
    private Canvas canvas;
    private JLabel statusBar;

    public RenderWindow(String title, Canvas canvas) throws HeadlessException {
        super(title);
        this.canvas = canvas;
        thisFrame = this;
        createGUI();
        canvas.setStatusLabel(statusBar);

        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        pack();
    }

    private void createGUI() {
        JPanel mainPanel = new JPanel(new BorderLayout());
        setContentPane(mainPanel);

        JToolBar toolBar = new JToolBar();
        mainPanel.add(toolBar, BorderLayout.PAGE_START);

        JButton zoomInBtn = new JButton("Zoom in");
        toolBar.add(zoomInBtn);

        JButton zoomOutBtn = new JButton("Zoom out");
        toolBar.add(zoomOutBtn);

        ///////////////////////// Canvas
        mainPanel.add(canvas, BorderLayout.CENTER);
        mainPanel.setPreferredSize(PREFERREDSIZE);

        //////////////////////// Status bar
        statusBar = new JLabel(" ");
        mainPanel.add(statusBar, BorderLayout.PAGE_END);

        //////////////////////// Actions
        zoomInBtn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                canvas.zoomIn();
                //canvas.setZoomCoefficient(zoomoefficient / ZOOM_STEP);
                repaint();
            }
        });

        zoomOutBtn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                canvas.zoomOut();
                //zoomCoefficient *= ZOOM_STEP;
                repaint();
            }
        });
    }


}

abstract class Canvas extends JPanel {

    protected static final double ZOOM_STEP = 0.8;
    protected static final int RULER_WIDTH = 10;

    protected Point dragStrat;
    protected Point origin = new Point(0, 0);
    protected Point dragOrigin = new Point(0, 0);
    protected double zoomCoefficient = 1;
    protected JFrame parent;
    protected DimensionInfo dimension1;
    protected DimensionInfo dimension2;
    protected int d1;
    protected int d2;

    protected JLabel statusLabel;
    protected Collection<Rule>rules;


    public Canvas(JFrame parent, int width, int height, List<DimensionInfo> dimensions) {
        this.parent = parent;
        setPreferredSize(new Dimension(width, height));
        MouseAdapter ma = new HandleMouseAdapter();
        addMouseListener(ma);
        addMouseMotionListener(ma);
        ///caclulate initial zoomCoefficient
        if (dimensions.size() > 2) {
            //need to select
            DimensionSelectionDialog selectionDialog = new DimensionSelectionDialog(parent, dimensions);
            selectionDialog.setVisible(true);
            if (selectionDialog.getReturnCode() == DimensionSelectionDialog.OK_BUTTON) {
                dimension1 = selectionDialog.getDimension1();
                dimension2 = selectionDialog.getDimension2();
            } else {
                dimension1 = dimensions.get(0);
                dimension2 = dimensions.get(1);
            }
        } else if (dimensions.size() == 2) {
            dimension1 = dimensions.get(0);
            dimension2 = dimensions.get(1);
        } else if (dimensions.size() == 1) {
            dimension1 = dimension2 = dimensions.get(0);

        } else {
            dimension1 = dimension2 = null;
            throw new RuntimeException("null dimension");
        }
        d1=Util.getDimensionInfoIndex(dimension1);
        d2=Util.getDimensionInfoIndex(dimension2);

    }

    public void setStatusLabel(JLabel statusLabel) {
        this.statusLabel = statusLabel;
    }

    public abstract void zoomIn();

    public abstract void zoomOut();

    private class HandleMouseAdapter extends MouseAdapter {

        public void mouseDragged(MouseEvent e2) {
            super.mousePressed(e2);
            origin.setLocation((int) (-(e2.getX() - dragStrat.getX()) + dragOrigin.getX()),
                    (int) (e2.getY() - dragStrat.getY() + dragOrigin.getY()));
            repaint();
        }

        public void mousePressed(MouseEvent e2) {
            super.mousePressed(e2);
            dragStrat = e2.getPoint();
            dragOrigin.setLocation(origin.getX(), origin.getY());
        }

        @Override
        public void mouseMoved(MouseEvent e) {
            statusLabel.setText(dimension1 + ": " + (int)((e.getX() - RULER_WIDTH + origin.getX()) / zoomCoefficient) + ", " +
                    dimension2 + ": " + (int)((e.getY() - RULER_WIDTH - origin.getY()) / zoomCoefficient));
        }
    }


}
