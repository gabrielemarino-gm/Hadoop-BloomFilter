package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class BloomFilter {
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: BloomFilter <input path> <output path> ");
            System.exit(-1);
        }

        if (!constructionJob(conf, otherArgs[0], "tmp1"))
            System.exit(-1);

        boolean finalStatus = bloomFilterJob(conf, "tmp1", otherArgs[0], otherArgs[1]);
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
        job.setJarByClass(ConstructionMR.class);

        // set mapper/reducer
        job.setMapperClass(ConstructionMR.ConstructionMapper.class);
        job.setReducerClass(ConstructionMR.ConstructionReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // define I/O
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //FileInputFormat.addInputPath(job, new Path(inPath));
        NLineInputFormat.addInputPath(job, new Path(inPath));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 311782);
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        return job.waitForCompletion(true);
    }

    private static boolean bloomFilterJob(Configuration conf, String inDataPath, String inPath, String outPath) throws Exception {
        int[] m = readM(conf, inDataPath, "");
        conf.setInt("m_1", m[0]);
        conf.setInt("m_2", m[1]);
        conf.setInt("m_3", m[2]);
        conf.setInt("m_4", m[3]);
        conf.setInt("m_5", m[4]);
        conf.setInt("m_6", m[5]);
        conf.setInt("m_7", m[6]);
        conf.setInt("m_8", m[7]);
        conf.setInt("m_9", m[8]);
        conf.setInt("m_10", m[9]);
        Job job = Job.getInstance(conf, "BloomFilterMR");
        //job.setJarByClass(InMemoryMovingAverage.class);

        // set mapper/reducer
        job.setMapperClass(BloomFilterMR.BloomFilterMapper.class);
        job.setReducerClass(BloomFilterMR.BloomFilterReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // define I/O
        //job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //FileInputFormat.addInputPath(job, new Path(inPath));
        NLineInputFormat.addInputPath(job, new Path(inPath));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 311782);
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        return job.waitForCompletion(true);
    }

    private static int[] readM(Configuration conf, String pathString, String pattern) throws Exception {
        int result[] = new int[7];
        FileSystem hdfs = FileSystem.get(conf);

        BufferedReader br= new BufferedReader(new InputStreamReader(hdfs.open(new Path(pathString))));
        try {
            String line;
            line=br.readLine();
            while (line != null){
                if (line.startsWith(pattern)) {
                    String[] inputs = line.split("\t");
                    int i = Integer.parseInt(inputs[0]);
                    result[i-1] = Integer.parseInt(inputs[1]);
                    //break;
                }

                // be sure to read the next line otherwise we get an infinite loop
                line = br.readLine();
            }
        } finally {
            // close out the BufferedReader
            br.close();
        }

        return result;
    }

    /*********************UTILS************************/
    public static class IntArrayWritable extends ArrayWritable {

        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }

        @Override
        public IntWritable[] get() {
            return (IntWritable[]) super.get();
        }

        @Override
        public String toString() {
            IntWritable[] values = get();
            return values[0].toString() + ", " + values[1].toString();
        }
    }
}
