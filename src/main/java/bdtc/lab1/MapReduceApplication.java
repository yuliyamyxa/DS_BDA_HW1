package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

@Log4j
public class MapReduceApplication {

    /**
     * Entry point for the application
     *
     * @param args Optional arguments: InputDirectory, OutputDirectory
     * @throws Exception when error occurred
     */
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            throw new RuntimeException("You should specify input, output directories and scale!");
        }
        Configuration conf = new Configuration();
        conf.set("metricScale", args[2]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);

        job.addCacheFile(new Path("hdfs://localhost:9000/user/root/input/devices").toUri());

        job.setMapOutputKeyClass(CustomType.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(CustomType.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.MALFORMED);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
    }
}
