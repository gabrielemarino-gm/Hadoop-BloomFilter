package it.unipi.hadoop;

import it.unipi.hadoop.BloomFilterMR;
import it.unipi.hadoop.ConstructionMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class BloomFilter
{
    private static final int NUM_BLOOM_FILTERS = 10;
    private static final int k = 7;

    public static void main(String[] args) throws Exception
    {
        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2)
        {
            System.err.println("Usage: BloomFilter <input path> <output path> ");
            System.exit(-1);
        }

        if (!constructionJob(conf, otherArgs[0], "tmp1")) // Here we wait for the execution of the first MapReduce algorithm
            System.exit(-1);

        boolean finalStatus = !bloomFilterJob(conf, "tmp1/part-r-00000", otherArgs[0], otherArgs[1]);
        //removeDirectory(conf, "tmp1");

        // TIME
        long end = System.currentTimeMillis();
        end -= start;
        System.out.println("EXECUTION TIME: " + end + " ms");

        if (!finalStatus)
            System.exit(-1);

        // System.out.println("TESTING THE FALSE POSITIVE RATES");
        // testJob(conf, otherArgs[1]);

    }

    private static boolean constructionJob(Configuration conf, String inPath, String outPath) throws Exception
    {
        Job job = Job.getInstance(conf, "ConstructionMR");
        job.setJarByClass(BloomFilter.class);

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

        // FileInputFormat.addInputPath(job, new Path(inPath));
        NLineInputFormat.addInputPath(job, new Path(inPath));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 311782);
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        return job.waitForCompletion(true);
    }

    private static boolean bloomFilterJob(Configuration conf, String inDataPath, String inPath, String outPath) throws Exception
    {
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

        //  ------------------------------- TEST --------------------------- //
        /*System.out.println();
        System.out.println();

        System.out.println("TEST HASH");
        int mario = 0;
        int index = (int) Math.round((7.5));

        try
        {
            mario = Integer.parseInt(conf.get("m_" + index));
        }
        catch (Exception e)
        {
            System.out.println("Error in parsing the input file");
        }

        System.out.println("Trovata per 8, m = " + mario);
        String movie_name = "tt123456789";
        // take m from the configuration
        Hash h = new MurmurHash();

        System.out.println("Lunghezza nome :" + movie_name.length());
        System.out.println("Lunghezza byte :" + movie_name.getBytes(StandardCharsets.UTF_8).length);
        int myHash = h.hash(movie_name.getBytes(StandardCharsets.UTF_8), movie_name.length(), 1);
        System.out.println("Senza modulo h(" + movie_name + ") = " + myHash);

        // (a % b + b) % b
        myHash = (myHash%mario + mario) % mario;
        System.out.println("h(" + movie_name + ") = " + myHash);
        System.out.println();
        System.out.println();*/
        //  ------------------------------- END TEST --------------------------- //



        Job job = Job.getInstance(conf, "BloomFilterMR");
        job.setJarByClass(BloomFilter.class);

        // set mapper/reducer
        job.setMapperClass(BloomFilterMR.BloomFilterMapper.class);
        job.setReducerClass(BloomFilterMR.BloomFilterReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BloomFilter.IntArrayWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        //alternative: the output is an array of int
        job.setOutputValueClass(BloomFilter.IntArrayWritable.class);

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

    
    private static void testJob(Configuration conf, String inDataPath) throws IOException
    {
        // TODO 30/05/2022: Test the bloom filter creation, then get the bloom filters and calculate the false postive rates
        FileSystem hdfs = FileSystem.get(conf);
        double falsePositives[] = new double[10];
        double trueNegatives [] = new double[10];
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inDataPath))));

        try
        {
            String line;
            line = br.readLine();

            while (line != null)
            {
                String[] inputs = line.split("\t");
                int i = Integer.parseInt(inputs[0]);
                for(int j = 0; j < k; j++)
                {
                    // TODO 30/05/2022: Apply the hash functions to find the position of the element in the filter

                }

                // be sure to read the next line otherwise we get an infinite loop
                line = br.readLine();
            }
        }
        finally
        {
            // close out the BufferedReader
            br.close();
        }
    }
    
    private static int[] readM(Configuration conf, String pathString, String pattern) throws Exception
    {
        int[] result = new int[NUM_BLOOM_FILTERS];
        FileSystem hdfs = FileSystem.get(conf);

        BufferedReader br= new BufferedReader(new InputStreamReader(hdfs.open(new Path(pathString))));

        try
        {
            String line;
            line=br.readLine();

            while (line != null)
            {
                if (line.startsWith(pattern))
                {
                    String[] inputs = line.split("\t");
                    int i = Integer.parseInt(inputs[0]);
                    result[i-1] = Integer.parseInt(inputs[1]);
                    // System.out.println("Result: " + result[i-1]);
                    //break;
                }

                // be sure to read the next line otherwise we get an infinite loop
                line = br.readLine();
            }
        }
        finally
        {
            // close out the BufferedReader
            br.close();
        }


        return result;
    }

    /*********************UTILS************************/
    public static class IntArrayWritable extends ArrayWritable
    {

        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }

        @Override
        public IntWritable[] get()
        {
            Writable[] tmp = super.get();
            if(tmp != null)
            {
                int n = tmp.length;
                IntWritable[] items = new IntWritable[n];
                for(int i = 0; i < tmp.length; i++)
                {
                    items[i] = (IntWritable)tmp[i];
                }
                return items;
            }
            else
            {
                return null;
            }
            //return (IntWritable[]) super.get();
        }


        /*public int[] getData()
        {
            IntWritable[] data = (IntWritable[]) super.get();
            int[] result = new int[data.length];
            for(int i = 0; i < data.length; i++)
            {
                result[i] = data[i].get();
            }

            return result;
        }*/

        public void set(int[] array)
        {
            IntWritable[] values = new IntWritable[array.length];

            for (int i=0; i<array.length; i++)
            {
                int temp = array[i];
                values[i] = new IntWritable();
                values[i].set(temp);
            }

            super.set(values);
        }

        @Override
        public String toString()
        {
            IntWritable[] values = (IntWritable[]) super.get();
            if(values.length == 0)
            {
                return "";
            }

            StringBuilder sb  = new StringBuilder();
            for(IntWritable value : values)
            {
                int i = value.get();
                sb.append(i).append(" ");
            }

            sb.setLength(sb.length()-1);
            return sb.toString();
            //return values[0].toString() + ", " + values[1].toString();
        }
    }
}

/*
-Contains the main method which calls the drivers of the bloom filters, utility methods for bloom filter consttuction and the false
positive rate test
*/
