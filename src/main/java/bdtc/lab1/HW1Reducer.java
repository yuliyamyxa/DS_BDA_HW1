package bdtc.lab1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer class
 * Input key {@link CustomType}
 * Input value {@link FloatWritable}
 * Output key {@link CustomType}
 * Output value {@link FloatWritable}
 */
public class HW1Reducer extends Reducer<CustomType, FloatWritable, CustomType, FloatWritable> {



    /**
     * override map map function to average metrics
     * @param device key
     * @param values iterable of values
     * @param context reducer context
     * @throws IOException in context.write()
     * @throws InterruptedException in context.write()
     */
    @Override
    protected void reduce(CustomType device, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0.0F;
        float count = 0.0F;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            count += 1;
        }
        context.write(new CustomType(device.getDeviceName(), device.getTimestamp(), device.getScaleText()),
                new FloatWritable(sum/count));
    }
}