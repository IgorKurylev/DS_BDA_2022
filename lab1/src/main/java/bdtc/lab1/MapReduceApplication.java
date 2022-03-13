package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.concurrent.TimeUnit;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            throw new RuntimeException("You should specify input, output and config folders!");
        }

        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        ConfigParser.readAndSetZones(new Path(args[2]), conf);
        ConfigParser.readAndSetTemperatures(new Path(args[2]), conf);

        Job job = Job.getInstance(conf, "mouse clicks heatmap");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.UNKNOWN_ZONE);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");

        long endTime = System.currentTimeMillis();
        long elapsedTime = TimeUnit.MILLISECONDS.toSeconds(endTime - startTime);

        log.info("Elapsed time: " + elapsedTime + " seconds");
    }
}
