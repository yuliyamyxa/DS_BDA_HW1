package bdtc.lab1;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Mapper class
 * Input key {@link LongWritable}
 * Input value {@link Text}
 * Output key {@link CustomType}
 * Output value {@link FloatWritable}
 */
public class HW1Mapper extends Mapper<LongWritable, Text, CustomType, FloatWritable> {
    private int scale;
    private String scaleText;
    private Map<Integer, String> deviceName;
    private final CustomType correctMetric = new CustomType();
    private final FloatWritable correctFloat = new FloatWritable();

    /**
     * Regex pattern to check if the input row is correct
     */
    private final static Pattern regular = Pattern.compile("^\\d+,\\d+,\\d+");

    /**
     * Regex pattern to split input row
     */
    private final static Pattern splitter = Pattern.compile(",|-");

    /**
     * function to resolve metric names
     * @param context hadoop configuration class
     * @return Map deviceMap
     * @throws IOException failed to get URI or parse file
     */
    public static Map<Integer, String> getDevicesNames(Context context) throws IOException {
        URI[] paths = context.getCacheFiles();
        Path path = null;
        for (URI u : paths) {
            if (u.getPath().toLowerCase().contains("devices")) {
                path = new Path(u.getPath());
            }
        }
        if (path != null) {
            Map<Integer, String> deviceMap = new HashMap<>();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FSDataInputStream(fs.open(path))));
            String line;
            int deviceId;
            while ((line = reader.readLine()) != null){
                try {
                    String[] parts = splitter.split(line);
                    deviceId = Integer.parseInt(parts[0]);
                    deviceMap.put(deviceId, parts[1]);
                }
                catch (NumberFormatException noexcept) {
                    throw new IOException();
                }
            }
            return deviceMap;
        } else {
            throw new IOException();
        }
    }

    /**
     * Initial mapper setup
     * @param context mapper context
     * @throws IOException failed to parse metric file or scale
     */
    @Override
    protected void setup(Context context) throws IOException{
        deviceName = getDevicesNames(context);
        scaleText = context.getConfiguration().get("metricScale");
        try {
            int value = Integer.parseInt(scaleText.substring(1));
            String measure = scaleText.substring(0, 1);

            switch (measure) {
                case "h":
                    scale = value * 3600 * 1000;
                    break;
                case "m":
                    scale = value * 60 * 1000;
                    break;
                case "s":
                    scale = value * 1000;
                    break;
            }
        }
        catch (RuntimeException noexcept){
            throw new IOException();
        }
    }

    /**
     * override map map function to agregate metrics
     * Uses counters {@link CounterType}
     * @param key input key
     * @param value input value
     * @param context mapper context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int deviceId;
        long timestamp;
        float metricValue;
        String line = value.toString();
        if (regular.matcher(line).matches()){
            String[] parts = splitter.split(line);
            deviceId = Integer.parseInt(parts[0]);
            timestamp = Long.parseLong(parts[1]) - Long.parseLong(parts[1]) % scale;
            metricValue = Float.parseFloat(parts[2]);

            if (deviceName.containsKey(deviceId)){
                correctMetric.setDeviceName(deviceName.get(deviceId));
                correctMetric.setTimestamp(timestamp);
                correctMetric.setScaleText(scaleText);
                correctFloat.set(metricValue);
                try {
                    context.write(correctMetric, correctFloat);
                } catch (RuntimeException noexcept) {
                    context.getCounter(CounterType.MALFORMED).increment(1);
                }
            } else context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}