import bdtc.lab1.CounterType;
import bdtc.lab1.CustomType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, CustomType, FloatWritable> mapDriver;

    private final String incorrectData = "Some incorrect data, 1";
    private final String correctData = "5,1610312470,1";


    @Before
    public void setup(){
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = new MapDriver<>(mapper);
        mapDriver.addCacheFile(new File("src/test/input_data/devices").getAbsolutePath());
        Configuration conf = mapDriver.getConfiguration();
        conf.set("metricScale", "s10");
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(incorrectData))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(correctData))
                .withOutput(new CustomType("Device_5", 1610310000, "s10"),
                        new FloatWritable(((float)1.0)))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(correctData))
                .withInput(new LongWritable(), new Text(incorrectData))
                .withInput(new LongWritable(), new Text(incorrectData))
                .withOutput(new CustomType("Device_5", 1610310000, "s10"),
                        new FloatWritable(((float) 1.0)))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}