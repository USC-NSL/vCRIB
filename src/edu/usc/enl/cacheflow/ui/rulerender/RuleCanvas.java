package edu.usc.enl.cacheflow.ui.rulerender;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.ui.DrawableRuleSpace;

import javax.swing.*;
import java.awt.*;
import java.util.Collections;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/29/11
 * Time: 12:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class RuleCanvas extends Canvas {
    private List<? extends DrawableRuleSpace> rules;

    public RuleCanvas(JFrame parent, int width, int height, List<DimensionInfo> ranges, List<? extends DrawableRuleSpace> rules) {
        super(parent, width, height, ranges);
        this.rules = rules;
        Collections.reverse(this.rules);


        long range1 = dimension1.getDimensionRange().getSize();
        long range2 = dimension2.getDimensionRange().getSize();
        zoomCoefficient = Math.min(1.0 * width / range1, 1.0 * height / range2);
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        Graphics2D g2 = (Graphics2D) g;
        //g2.scale(zoomCoefficient, zoomCoefficient); // moved into rule painting to be able to draw large numbers
        //g2.transform(new AffineTransform(1, 0, 0, -1, 0, 0));


        g2.translate(-origin.getX() + RULER_WIDTH, origin.getY() + RULER_WIDTH);
        for (DrawableRuleSpace rule : rules) {
            rule.paint(g2, d1, d2, zoomCoefficient);
        }


        RangeDimensionRange range1 = dimension1.getDimensionRange();
        long start1 =  range1.getStart();
        long end1 = range1.getEnd();
        int nums1 = (int) (zoomCoefficient * (end1 - start1) / 40);

        g2.drawLine((int) (zoomCoefficient * start1), 0, (int) (zoomCoefficient * end1), 0);
        for (int i = 0; i < nums1; i++) {
            long digit = start1 + i * (end1 - start1) / nums1;
            int x1 = (int) (zoomCoefficient * digit);
            g2.drawLine(x1, -RULER_WIDTH, x1, 0);
            g2.drawString("" + digit, x1 + 1, 0);

        }
        RangeDimensionRange range2 = dimension2.getDimensionRange();
        long start2 = range2.getStart();
        long end2 = range2.getEnd();
        int nums2 = (int) (zoomCoefficient * (end1 - start1) / 40);

        g2.drawLine(0, (int) (zoomCoefficient * start2), 0, (int) (zoomCoefficient * end2));
        for (int i = 0; i < nums2; i++) {
            long digit = start2 + i * (end2 - start2) / nums2;
            int x1 = (int) (zoomCoefficient * digit);

            g2.drawLine(0, x1, -RULER_WIDTH, x1);
            g2.rotate(-Math.PI / 2);
            g2.drawString("" + digit, (int) (zoomCoefficient * (-digit)) + 1, 0);//because of rotation x and y are interchanged
            g2.rotate(Math.PI / 2);
        }

    }


    @Override
    public void zoomIn() {
        zoomCoefficient /= ZOOM_STEP;

    }

    @Override
    public void zoomOut() {
        zoomCoefficient *= ZOOM_STEP;
    }
}
