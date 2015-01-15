package edu.usc.enl.cacheflow.ui.rulerender;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/29/11
 * Time: 11:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class PartitionCanvas extends edu.usc.enl.cacheflow.ui.rulerender.Canvas {
    private RuleCanvas rulesView;
    private RuleCanvas partitionsView;

    public PartitionCanvas(JFrame parent, int width, int height, List<DimensionInfo> dimensions, List<Partition> partitions) {
        super(parent, width, height, dimensions);
        setLayout(new GridLayout(1, 2, 10, 1));

        Set<Rule> rulesss=new HashSet<Rule>();
        List<Rule> fineRules = new LinkedList<Rule>();
        for (Partition partition : partitions) {
            fineRules.addAll(partition.getRules());
            rulesss.addAll(partition.getRules());
        }
        Collections.sort(fineRules,Rule.PRIORITY_COMPARATOR);
        rulesView = new RuleCanvas(parent, width / 2, height, dimensions, fineRules);
        //rulesView.setBorder(BorderFactory.createTitledBorder("FineRules"));
        add(rulesView);

        partitionsView = new RuleCanvas(parent, width / 2, height, dimensions, partitions);
        //partitionsView.setBorder(BorderFactory.createTitledBorder("Partitions"));
        add(partitionsView);

        rulesView.origin = partitionsView.origin;
        rulesView.addMouseMotionListener(new ConnectorMouseAdapter(partitionsView));
        partitionsView.addMouseMotionListener(new ConnectorMouseAdapter(rulesView));
    }

    @Override
    public void zoomIn() {
        rulesView.zoomIn();
        partitionsView.zoomIn();
    }

    @Override
    public void setStatusLabel(JLabel statusLabel) {
        super.setStatusLabel(statusLabel);
        rulesView.setStatusLabel(statusLabel);
        partitionsView.setStatusLabel(statusLabel);
    }

    @Override
    public void zoomOut() {
        rulesView.zoomOut();
        partitionsView.zoomOut();
    }

    private class ConnectorMouseAdapter extends MouseAdapter {
        private edu.usc.enl.cacheflow.ui.rulerender.Canvas otherCanvas;

        private ConnectorMouseAdapter(edu.usc.enl.cacheflow.ui.rulerender.Canvas otherCanvas) {
            this.otherCanvas = otherCanvas;
        }

        public void mouseDragged(MouseEvent e2) {
            otherCanvas.repaint();
        }

    }
}
