import bdtc.lab1.CustomType;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, CustomType, FloatWritable> mapDriver;
    private ReduceDriver<CustomType, FloatWritable, CustomType, FloatWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, CustomType, FloatWritable, CustomType, FloatWritable> mapReduceDriver;

    private final String correctData = "5,1610312470,1";

    @Before
    public void setup(){
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = new MapDriver<>(mapper);
        mapDriver.addCacheFile(new File("src/test/input_data/devices").getAbsolutePath());
        Configuration conf = mapDriver.getConfiguration();
        conf.set("metricScale", "s10");

        HW1Reducer reducer= new HW1Reducer();
        reduceDriver = new ReduceDriver<>(reducer);
        reduceDriver.addCacheFile(new File("src/test/input_data/devices").getAbsolutePath());
        conf = reduceDriver.getConfiguration();
        conf.set("metricScale", "s10");

        mapReduceDriver = new MapReduceDriver<>(mapper, reducer);
        mapReduceDriver.addCacheFile(new File("src/test/input_data/devices").getAbsolutePath());
        conf = mapReduceDriver.getConfiguration();
        conf.set("metricScale", "s10");
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(correctData))
                .withOutput(new CustomType("Device_5", 1610310000, "s10"), new FloatWritable(((float) 1.0)))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<FloatWritable> iterable = new ArrayList<>();
        iterable.add(new FloatWritable(10.0F));
        iterable.add(new FloatWritable(50.0F));
        reduceDriver
                .withInput(new CustomType("Device_5", 1610310000, "s10"), iterable)
                .withOutput(new CustomType("Device_5", 1610310000, "s10"),
                        new FloatWritable(30.0F))
                .runTest();
    }

    @Test
    public void testMapperAndReducer() throws IOException {
        String correctRow = "5,1610312350,5";
        String incorrectRow = "Some incorrect data,1";
        String incorrectDevice = "6,1610314290,200";
        mapReduceDriver
                .withInput(new LongWritable(), new Text(correctData))
                .withInput(new LongWritable(), new Text(incorrectRow))
                .withInput(new LongWritable(), new Text(correctRow))
                .withInput(new LongWritable(), new Text(incorrectDevice))
                .withOutput(new CustomType("Device_5", 1610310000, "s10"),
                        new FloatWritable(3.0F))
                .runTest();
    }
}