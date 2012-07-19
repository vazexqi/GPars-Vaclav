package groovyx.gpars.benchmark.caliper.chart

import com.google.caliper.model.Environment
import com.google.caliper.model.Instrument
import com.google.caliper.model.Measurement
import com.google.caliper.model.Run
import groovy.xml.MarkupBuilder

import java.text.DateFormat
import java.text.SimpleDateFormat

class HTMLBuilder {
    private Run run

    public HTMLBuilder(Run run) {
        this.run = run

    }

    public void buildLineGraphURL(ArrayList<String> xValues, ArrayList<Long> yValues, List<ArrayList<String>> historyXValues, List<ArrayList<Long>> historyYValues,
                                  ArrayList<String> historyNames, String xLabel, String yLabel, long globalMax) {

        def chart = new GoogleChartBuilder()
        String result = chart.lineXY {
            size(w: 750, h: 400)
            title {
                row(run.label)
            }
            data(encoding: 'text', numLines: (historyXValues.size() + 1) * 2) {
                set(xValues.toList())
                set(yValues.toList())
                for (int i = 0; i < historyXValues.size(); i++) {
                    set(historyXValues[i].toList())
                    set(historyYValues[i].toList())
                }
            }
            colors {
                Random rand = new Random();

                for (int i = 0; i < historyXValues.size() + 1; i++) {
                    String colorHex = "";
                    for (int j = 0; j < 3; j++) {
                        char c = 48 + (rand.nextInt(9))
                        colorHex = colorHex << c << c
                    }
                    color(colorHex)
                }
            }
            //lineStyle(line1:[1,6,3])
            legend {
                label("Current")
                historyNames.each {
                    label(it)
                }
            }
            axis(bottom: [], left: [], left2: [yLabel], bottom2: [xLabel])
            range([0: [xValues.get(0).toInteger(), xValues.get(xValues.size() - 1).toInteger()], 1: [1, globalMax]])
            dataRange('a')
        }
        buildHTML(result)
    }

    public void buildBarGraphURL(ArrayList<String> xValues, ArrayList<Long> yValues, List<ArrayList<String>> historyXValues, List<ArrayList<Long>> historyYValues,
                                 ArrayList<String> historyNames, String xLabel, String yLabel, long globalMax) {

        def chart = new GoogleChartBuilder()
        int numberOfLines = yValues.size() * (historyYValues.size() + 1)
        int margin = 500 / numberOfLines
        int width = 0, space = 0
        if (margin > 30) {
            width = 30
            space = 1
        }
        else {
            space = 1
            width = margin - space
        }
        String result = chart.bar(['vertical', 'grouped']) {
            size(w: 750, h: 400)
            barSize(width: width, space: space)
            title {
                row(run.label)
            }
            data(encoding: 'text', numLines: (historyXValues.size() + 1) * 2) {
                set(yValues.toList())
                for (int i = 0; i < historyXValues.size(); i++) {
                    set(historyYValues[i].toList())
                }
            }
            colors {
                color('FF9966')
                color('6699FF')
                color('99FF66')
                color('66CC00')
            }
            legend {
                label("Current")
                historyNames.each {
                    label(it)
                }
            }
            axis(bottom: xValues.toList(), left: [])
            range([0: [xValues.get(0).toInteger(), xValues.get(xValues.size() - 1).toInteger()], 1: [1, globalMax]])
            dataRange('a')
            labelOption('b')
        }
        buildHTML(result)
    }

    void buildHTML(String url) {
        File dir = new File("caliper-charts");
        dir.mkdir();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmm");
        Date date = new Date();
        FileWriter writer = new FileWriter(dir.name + File.separator + run.label + '' + dateFormat.format(date) + '.html')
        def builder = new MarkupBuilder(writer)
        builder.html {
            head {
                title run.label + " " + dateFormat.format(date)
            }
            body {
                table {//results table
                    tr {
                        th(style: "font-size: 0.75em", "Number of Actors")
                        th(style: "font-size: 0.75em", "Measurements")
                    }
                    for (int i = 0; i < run.scenarios.size(); i++) {
                        tr {
                            Measurement m = run.results.get(i).measurements.get(0);
                            td(style: "font-size: 0.75em", run.scenarios.get(i).userParameters.get("numberOfClients"));
                            td(style: "font-size: 0.75em", ((long) m.@value / m.weight));
                        }
                    }
                    tr {
                        img(src: url, border: 0)
                    }
                }
                table {
                    tr {
                        th(style: "font-size: 0.75em", "Environment")
                    }
                    for (int i = 0; i < run.environments.size(); i++) {
                        Environment e = run.environments.get(i)
                        SortedMap<String, String> properties = e.@properties
                        tr {
                            td(style: "font-size: 0.75em", "CPU: " + properties.get("host.cpu.names"))
                        }
                        tr {
                            td(style: "font-size: 0.75em", "Number of Cores: " + properties.get("host.cpus"))
                        }
                        tr {
                            td(style: "font-size: 0.75em", "Memory: " + properties.get("host.memory.physical"))
                        }
                        tr {
                            td(style: "font-size: 0.75em", "OS: " + properties.get("os.name") + " " + properties.get("os.version"))
                        }
                    }
                }
                table {
                    for (int i = 0; i < run.vms.size(); i++) {
                        tr {
                            td(style: "font-size: 0.75em", "VM: " + run.vms.get(i).vmName)
                        }
                    }
                }
                table {
                    for (int i = 0; i < run.instruments.size(); i++) {
                        Instrument instrument = run.instruments.get(i);
                        tr {
                            String s = instrument.className
                            td(style: "font-size: 0.75em", "Instrument: " + s.substring(s.lastIndexOf('.') + 1, s.length()))
                        }
                    }
                }
            }
        }
        writer.flush()
        writer.close()
    }
}
