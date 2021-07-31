import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import java.awt.*;
import java.util.List;

public class Chart extends JFrame {

    private List<Double> xData;
    private List<Double> yData;
    private double functionA;
    private double functionB;
    private String xLabel;
    private String yLabel;

    public Chart() {

    }

    public void showChart(Double observedValue, Double predictedValue) {
        XYDataset dataset = createDataset(observedValue, predictedValue);

        JFreeChart chart = createChart(dataset);

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        chartPanel.setBackground(Color.white);
        add(chartPanel);

        pack();
        setTitle("Linear regression");
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setVisible(true);
    }

    private XYDataset createDataset(Double observedValue, Double predictedValue) {
        XYSeries data = new XYSeries("Data");

        double minX = this.xData.get(0);
        double maxX = this.xData.get(0);
        for (int i = 0; i < this.xData.size(); i++) {
            System.out.print("Plotting " + i + "/" + xData.size() + "\r");
            data.add(this.xData.get(i), this.yData.get(i));
            minX = this.xData.get(i) < minX ? this.xData.get(i) : minX;
            maxX = this.xData.get(i) > maxX ? this.xData.get(i) : maxX;
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(data);

        XYSeries trend = new XYSeries("Trend");
        trend.add(minX, this.functionA * minX + this.functionB);
        trend.add(maxX, this.functionA * maxX + this.functionB);
        dataset.addSeries(trend);

        XYSeries predicted = new XYSeries("Predicted Value");
        predicted.add(observedValue, predictedValue);
        dataset.addSeries(predicted);

        return dataset;
    }

    private JFreeChart createChart(XYDataset dataset) {

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Linear Regression",
                xLabel,
                yLabel,
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        XYPlot plot = chart.getXYPlot();

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesLinesVisible(0, false);
        renderer.setSeriesLinesVisible(2, false);

        renderer.setSeriesPaint(0, Color.RED);
        renderer.setSeriesPaint(1, Color.BLUE);
        renderer.setSeriesPaint(2, Color.GREEN);

        plot.setRenderer(renderer);
        plot.setBackgroundPaint(Color.white);

        plot.setRangeGridlinesVisible(true);
        plot.setRangeGridlinePaint(Color.BLACK);

        plot.setDomainGridlinesVisible(true);
        plot.setDomainGridlinePaint(Color.BLACK);

        chart.getLegend().setFrame(BlockBorder.NONE);

        chart.setTitle(new TextTitle("Linear Regression",
                        new Font("Serif", java.awt.Font.BOLD, 18)
                )
        );

        return chart;
    }

    public void setxData(List<Double> xData) {
        this.xData = xData;
    }

    public void setyData(List<Double> yData) {
        this.yData = yData;
    }

    public void setFunctionA(double functionA) {
        this.functionA = functionA;
    }

    public void setFunctionB(double functionB) {
        this.functionB = functionB;
    }

    public void setxLabel(String xLabel) {
        this.xLabel = xLabel;
    }

    public void setyLabel(String yLabel) {
        this.yLabel = yLabel;
    }
}