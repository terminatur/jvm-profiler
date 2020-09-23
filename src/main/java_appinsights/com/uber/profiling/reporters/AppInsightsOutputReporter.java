package com.uber.profiling.reporters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.applicationinsights.TelemetryClient;
import com.microsoft.applicationinsights.TelemetryConfiguration;
import com.microsoft.applicationinsights.telemetry.SeverityLevel;
import org.apache.commons.lang3.StringUtils;


import com.uber.profiling.Reporter;
import com.uber.profiling.util.AgentLogger;

/**
 * Metrics reporter class for Azure Application Insights.
 *
 * To uses AppInsightsOutputReporter with default database connection properties
 * pass it in command.
 * 
 *     reporter=com.uber.profiling.reporters.AppInsightsOutputReporter
 * 
 * To use database connection properties from yaml file use below command. 
 * 
 *     reporter=com.uber.profiling.reporters.AppInsightsOutputReporter,configProvider=com.uber.profiling.YamlConfigProvider,configFile=/opt/influxdb.yaml
 *
 */
public class AppInsightsOutputReporter implements Reporter {

    private static final AgentLogger logger = AgentLogger.getLogger(AppInsightsOutputReporter.class.getName());

    private TelemetryClient telemetryClient;

    private String instrumentationKey = "<instrumentation-key>";

    public AppInsightsOutputReporter() {
        this.initilizeTelemetryClient();
    }

    private void initilizeTelemetryClient() {
        logger.info("Initializing telemetry client with instrumentation key: " + instrumentationKey);

        TelemetryConfiguration.getActive().setInstrumentationKey(instrumentationKey);
        telemetryClient = new TelemetryClient();
    }

    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        // format metrics 
        logger.debug("Profiler Name : " + profilerName);
        Map<String, String> formattedMetrics = getFormattedMetrics(metrics);
        for (Map.Entry<String, String> entry : formattedMetrics.entrySet()) {
            logger.debug("Formatted Metric-Name = " + entry.getKey() + ", Metric-Value = " + entry.getValue());
        }

        telemetryClient.trackTrace("cluster metrics", SeverityLevel.Information, formattedMetrics);
    }

    // Format metrics in key=value (line protocol)
    private Map<String, String> getFormattedMetrics(Map<String, Object> metrics) {
        Map<String, String> formattedMetrics = new HashMap<>();
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            logger.debug("Raw Metric-Name = " + key + ", Metric-Value = " + value);
            if (value != null && value instanceof List) {
                List listValue = (List) value;
                if (!listValue.isEmpty() && listValue.get(0) instanceof String) {
                    List<String> metricList = (List<String>) listValue;
                    formattedMetrics.put(key, String.join(",", metricList));
                } else if (!listValue.isEmpty() && listValue.get(0) instanceof Map) {
                    List<Map<String, Object>> metricList = (List<Map<String, Object>>) listValue;
                    int num = 1;
                    for (Map<String, Object> metricMap : metricList) {
                        String name = null;
                        if(metricMap.containsKey("name") && metricMap.get("name") != null && metricMap.get("name") instanceof String){
                            name = (String) metricMap.get("name");
                            name = name.replaceAll("\\s", "");
                        }
                        for (Map.Entry<String, Object> entry1 : metricMap.entrySet()) {
                            if(StringUtils.isNotEmpty(name)){
                                formattedMetrics.put(key + "-" + name + "-" + entry1.getKey(), entry1.getValue().toString());
                            }else{
                                formattedMetrics.put(key + "-" + entry1.getKey() + "-" + num, entry1.getValue().toString());
                           }
                        }
                        num++;
                    }
                }
            } else if (value != null && value instanceof Map) {
                Map<String, Object> metricMap = (Map<String, Object>) value;
                for (Map.Entry<String, Object> entry1 : metricMap.entrySet()) {
                    String key1 = entry1.getKey();
                    Object value1 = entry1.getValue();
                    if (value1 != null && value1 instanceof Map) {
                        Map<String, Object> value2 = (Map<String, Object>) value1;
                        int num = 1;
                        for (Map.Entry<String, Object> entry2 : value2.entrySet()) {
                            formattedMetrics.put(key + "-" + key1 + "-" + entry2.getKey() + "-" + num, entry2.getValue().toString());
                        }
                        num++;
                    }
                }
            } else {
                formattedMetrics.put(key, value.toString());
           }
        }
        return formattedMetrics;
    }

    @Override
    public void close() {
        synchronized (this) {
            this.telemetryClient.flush();
            try {
                Thread.sleep(5000);
            } catch (Exception ignored) {}
            this.telemetryClient = null;
        }
    }

    // properties from yaml file
    @Override
    public void updateArguments(Map<String, List<String>> connectionProperties) {
        for (Map.Entry<String,  List<String>> entry : connectionProperties.entrySet()) {
            String key = entry.getKey();
            List<String> value = entry.getValue();
            if (StringUtils.isNotEmpty(key) && value != null && !value.isEmpty()) {
                String stringValue = value.get(0);
                if (key.equals("appinsights.instrumentationkey")) {
                    logger.debug("Got value for host = "+stringValue);
                    this.instrumentationKey = stringValue;
                    this.initilizeTelemetryClient();
                }
            }
        }
    }
}
