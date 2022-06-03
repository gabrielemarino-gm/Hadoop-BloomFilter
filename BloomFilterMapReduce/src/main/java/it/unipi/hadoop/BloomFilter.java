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
    private static int k;
    private static double p;
    private static int[] m;

    public static void main(String[] args) throws Exception
    {
        long start = System.currentTimeMillis(); //starting time of execution

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3)
        {
            System.err.println("Usage: BloomFilter <input path> <output path> <false positive rate>");
            System.exit(-1);
        }

        if (!constructionJob(conf, otherArgs[0], "tmp1", otherArgs[2])) // Here we wait for the execution of the first MapReduce algorithm
            System.exit(-1);

        boolean finalStatus = !bloomFilterJob(conf, "tmp1/part-r-00000", otherArgs[0], otherArgs[1], otherArgs[2]);

        // TIME
        long end = System.currentTimeMillis(); //stoppage time of the execution
        end -= start;
        System.out.println("EXECUTION TIME: " + end + " ms");

        //TEST
        System.out.println("\nTESTING THE BLOOM FILTERS\n");
        String outFile = otherArgs[1] + "/part-r-00000";
        testJob(conf, otherArgs[0], outFile); //TODO 1/06/2022: try the test from the virtual cluster

        if (!finalStatus)
            System.exit(-1);

    }

    //starts the job for the construction of the bloom filters' parameters
    /**
     * Function which setups the job for the MapReduce algorithm which computes the parameters for the
     * bloom filters construction; the method computes the value of k using the input false positive rate
     * and sets it in the job configuration;the input is splitted based on the number of nodes in the cluster
     * @param  conf     the configuration to pass to the job
     * @param  inPath   path of the input file containing the dataset
     * @param  outPath  path where to store to the output file of the construction job
     * @param  fpr      the input value of the desired false positive rate
     * @return          status of completion of the job
     */
    private static boolean constructionJob(Configuration conf, String inPath, String outPath, String fpr) throws Exception
    {
        //extract p and set it to the configuration
        p = Double.parseDouble(fpr);
        conf.setDouble("p", p);
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
        //we set as number of lines to give to the mappers the total number of lines of the dataset
        //divided by the number of nodes of the cluster
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 311782);
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        return job.waitForCompletion(true);
    }

    //starts the job for the MapReduce algorithm for the Bloom Filters construction
    /**
     * Function which setups the job for the MapReduce algorithm which constructs the bloom filters;
     * the method retrieves the values of m from HDFS and puts to the configuration of the job;
     * the input is splitted based on the number of nodes in the cluster
     * @param  conf     the configuration to pass to the job
     * @param  inPath   path of the input file containing the values of m
     * @param  inPath   path of the input file containing the dataset
     * @param  outPath  path where to store to the output file of the bloom filter construction job
     * @param  fpr      the input value of the desired false positive rate
     * @return          status of completion of the job
     */
    private static boolean bloomFilterJob(Configuration conf, String inDataPath, String inPath, String outPath, String fpr) throws Exception
    {
        //call readM to take all the values of m obtained with the Constructor
        m = readM(conf, inDataPath, "");
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
        //compute k and set it to the configuration
        double nhash = (-1*Math.log(Double.parseDouble(fpr))/(Math.log(2)));
        k = (int) Math.ceil(nhash);
        conf.setInt("k", k);

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
        job.setOutputValueClass(BloomFilter.IntArrayWritable.class);

        // define I/O
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //FileInputFormat.addInputPath(job, new Path(inPath));
        NLineInputFormat.addInputPath(job, new Path(inPath));
        //we set as number of lines to give to the mappers the total number of lines of the dataset
        //divided by the number of nodes of the cluster
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 311782);
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        return job.waitForCompletion(true);
    }

    //launches the test of the false positive rates of the bloom filter constructed in the bloomFilterJob
    private static void testJob(Configuration conf, String inDataPath, String inBfPath) throws IOException
    {
        FileSystem hdfs = FileSystem.get(conf);
        double falsePositives[] = new double[10];
        double trueNegatives [] = new double[10];
        BufferedReader dataBr = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inDataPath)))); //to read the dataset
        BufferedReader bloomFilterBr= new BufferedReader(new InputStreamReader(hdfs.open(new Path(inBfPath)))); //to read the filters
        Hash h  = new MurmurHash(); //hash function family to use for the test
        int[][] bloomFilter = new int[10][]; //to store the bloom filters
        //for each filter we set the length to the value specified by m
        for (int i = 0; i < bloomFilter.length; ++i) {
            int tmp = m[i];
            bloomFilter[i] = new int[tmp];
        }

        //Otain the bloom filters from the file
        try
        {
            String line;
            line = bloomFilterBr.readLine();
            while (line != null)
            {
                String[] inputs = line.split("\t"); //key value split
                //we take the key and assing the bloom filter to the corresponding entry
                int i = Integer.parseInt(inputs[0]);
                String[] bfArray = inputs[1].split(" ");
                for(int j = 0; j < bfArray.length; j++) {
                    bloomFilter[i-1][j] = Integer.parseInt(bfArray[j]);
                }
                // be sure to read the next line otherwise we get an infinite loop
                line = bloomFilterBr.readLine();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            // close out the BufferedReader
            bloomFilterBr.close();
        }

        try
        {
            String line;
            line = dataBr.readLine();
            while (line != null)
            {
                String[] inputs = line.split("\t"); //take the values from the input entry
                String movie_name = inputs[0]; //movie id
                double rate = Double.parseDouble(inputs[1]); //take the rating
                int rating = (int) Math.round((rate)); //round the rating
                Boolean positive;
                for(int i = 0; i < bloomFilter.length; i++)
                {
                    positive = true; //initialize to true
                    for (int j = 0; j < k; j++)
                    {
                        //take the hash value for checking the elements
                        int pos = (h.hash(movie_name.getBytes(StandardCharsets.UTF_8), movie_name.length(), j) % m[i] + m[i]) % m[i];

                        //if there is not an element but it's not supposed to be there, then the element is a true negative
                        if ((bloomFilter[i][pos] == 0) && (i != rating - 1))
                        {
                            trueNegatives[i]++;
                            positive = false; //set to false in case we have a negative
                            break;
                        }
                    }
                    //if the element is in the filter but it shouldn't be there is a false positive
                    if(positive && i != rating -1) //positive is true if the value was not 0
                    {
                        falsePositives[i]++;
                    }
                }

                // be sure to read the next line otherwise we get an infinite loop
                line = dataBr.readLine();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            // close out the BufferedReader
            dataBr.close();
        }

        System.out.println("\n\n**********RESULTS**********\n\n");
        for(int i = 0; i < 10; i++)
        {
            //compute the false positive rate
            double fp_rate = falsePositives[i] / (falsePositives[i] + trueNegatives[i]);
            int j = i + 1;
            System.out.println("Rate " + j + ": False positives =  " + falsePositives[i] + ", FPR =  " + fp_rate  + "\n");
        }

    }
    
    //reads the values of m from the ouptut file of the configuration job
    /**
     * Function which read the values of m from HDFS
     * @param  conf         the configuration from which read the file containing the values of m
     * @param  pathString   path of the input file containing the values of m
     * @param  pattern      starting pattern of the desired lines of the file
     * @return              array with the values of m, ordered by key
     */
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
                    //split the input
                    String[] inputs = line.split("\t");
                    //take the rating
                    int i = Integer.parseInt(inputs[0]);
                    //assing to the correct position the m value associated to the rating
                    result[i-1] = Integer.parseInt(inputs[1]);
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

        //set the object with the values specified from the input array of int
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

        //returns the element at the given position
        public int getElemAt(int pos){
            Writable[] tmp = super.get();
            IntWritable elem = (IntWritable)tmp[pos];
            return elem.get();
        }

        //returns the length of the object
        public int getLen(){
            Writable[] tmp = super.get();
            return tmp.length;
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
        }
    }
}

/*
-Contains the main method which calls the drivers of the bloom filters, utility methods for bloom filter consttuction and the false
positive rate test
*/
