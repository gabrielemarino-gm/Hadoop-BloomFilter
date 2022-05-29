package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BloomFilter {
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: PageRank <input path> <output path> <# of iterations>");
            System.exit(-1);
        }

        if (!constructionJob(conf, otherArgs[0], "tmp1"))
            System.exit(-1);

        boolean finalStatus = bloomFilterJob(conf, "tmp2/iter" + (Integer.parseInt(otherArgs[2])), otherArgs[1]);
        //removeDirectory(conf, "tmp2");

        // TIME
        long end = System.currentTimeMillis();
        end -= start;
        System.out.println("EXECUTION TIME: " + end + " ms");

        if (!finalStatus)
            System.exit(-1);
    }

    private static boolean constructionJob(Configuration conf, String inPath, String outPath) throws Exception {

        Job job = Job.getInstance(conf, "ConstructionMR");
        //job.setJarByClass(InMemoryMovingAverage.class);

        // set mapper/reducer
        job.setMapperClass(BloomFilterMR.BloomFilterMapper1.class);
        job.setReducerClass(BloomFilterMR.BloomFilterReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(TimeSeriesData.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(inPath));
        //MultipleInputs.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean bloomFilterJob(Configuration conf, String inPath, String outPath) throws Exception {

        Job job = Job.getInstance(conf, "BloomFilterMR");
        //job.setJarByClass(InMemoryMovingAverage.class);

        // set mapper/reducer
        job.setMapperClass(BloomFilterMR.BloomFilterMapper1.class);
        job.setReducerClass(BloomFilterMR.BloomFilterReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(TimeSeriesData.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set window size for moving average calculation
        job.getConfiguration().set("m_1", "100");
        job.getConfiguration().set("m_2", "1000");
        job.getConfiguration().set("m_3", "1000");
        job.getConfiguration().set("m_4", "100");
        job.getConfiguration().set("m_5", "100");
        job.getConfiguration().set("m_6", "1000");
        job.getConfiguration().set("m_7", "1000");

        // define I/O
        FileInputFormat.addInputPath(job, new Path(inPath));
        //MultipleInputs.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }
}
