package it.unipi.hadoop;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.hash.MurmurHash;

public class BloomFilterMR
{

    public static class BloomFilterMapper1 extends Mapper<LongWritable, Text, Text, IntArrayWritable>
    {
        // reuse Hadoop's Writable objects
        private final Text outputKey = new Text();
        private final IntArrayWritable outputValue = new IntArrayWritable();
        private final int k = 7;
        //private final TimeSeriesData reducerValue = new TimeSeriesData();

        /*
        double[][] centroids = new double[5][];

        public void setup(Configuration conf) {
             InputStream is = FileSystem.get(conf).open(new Path("centroids.txt"));
             // read centroid values
        }

        If you don't want to hardcode file path in mapper code, you can pass it to configuration object before submitting the job:

        conf.set("centroids.path", "centroids.txt");
        and then read it in mapper

        InputStream is = FileSystem.get(conf).open(new Path(conf.get("centroids.path")));
         */

        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // To be implemented
			/*
			Algorithm 1 MAPPER
			Require: HASH F U N CT ION S, list of hash functions
			Require: input split, record in input to the Mapper
			Initialize movie name ←input split.movie name
			Initialize avg rating ←round next int(input split.avg rating)
			Initialize hash values = empty
			for each hash f unction ∈ HASH F U N CT ION S do
			hash values[pos] ← hash function(movie name)
			pos++;
			end for
			emit(avg rating, hash values)
			*/
            /*
            List<Hash> hashList = new ArrayList<>();
            String[] inputs = value.toString().split(" ");

            if(inputs.length < 3){
                return;
            }

            double key = 0;
            String movie_name = inputs[0];

            try{

                val = Double.parseDouble(inputs[3]);
            }
            catch(Exception e){
                System.out.println("Error in parsing the input file");
            }

            int size = numColM;
            if(label.equals("M")){
                size = numColN;
            }

            for(int k = 0; k < size; k++){
                outputKey.set(indexRow + "," + k);
                outputValue.set(label + "," + indexCol+ "," + val);
                context.write(outputKey, outputValue);
            }
             */

            String[] inputs = value.toString().split(" ");

            if(inputs.length < 3){
                return;
            }
            double rate = 0;
            String movie_name = inputs[0];
            String rating = "";
            try{
                rating = inputs[1];
                rate = Double.parseDouble(inputs[1]);
            }
            catch(Exception e){
                System.out.println("Error in parsing the input file");
            }

            if (rate == Math.floor(rate) && !Double.isInfinite(rate) && !rating.endsWith(".0"))
            {
                System.out.println("Error in parsing the input file");
            }
            int m = 0;
            try {
                m = Integer.parseInt(context.getConfiguration().get("m_" + rate));
            }
            catch (Exception e){
                System.out.println("Error in parsing the input file");
            }
            Hash h = new MurmurHash();
            IntWritable hashList[] = new IntWritable[k];
            for(int i = 0; i < k; i++) {
                //((int) (Math.random()*(maximum - minimum))) + minimum;
                int seed = (int)(Math.random()*(k-1))%m;
                hashList[i] = new IntWritable(h.hash(movie_name.getBytes(StandardCharsets.UTF_8), movie_name.length(), seed));
            }
            outputKey.set(String.valueOf((int) Math.round((rate))));

            outputValue.set(hashList);
            context.write(outputKey, outputValue);

        }
    }

    /*public static class BloomFilterMapper2 extends Mapper<LongWritable, Text, Text, Text>
    {
        // reuse Hadoop's Writable objects
        private final Text reducerKey = new Text();
        //private final TimeSeriesData reducerValue = new TimeSeriesData();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // To be implemented

			Algorithm 1 MAPPER
			Require: HASH F U N CT ION S, list of hash functions
			Require: input split, record in input to the Mapper
			Initialize movie name ←input split.movie name
			Initialize avg rating ←round next int(input split.avg rating)
			Initialize hash values = empty
			for each hash f unction ∈ HASH F U N CT ION S do
			hash values[pos] ← hash function(movie name)
			pos++;
			end for
			emit(avg rating, hash values)


        }
    }*/

    public static class BloomFilterReducer extends Reducer<Text, IntWritable, Text, Text>
    {
        private int windowSize;

        public void setup(Context context) throws IOException, InterruptedException
        {
            // To be implemented
            Configuration conf = context.getConfiguration();

        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            /*
			Algorithm 2 REDUCER
			Require: IN P U T LIST , list of hash results computed in the Mapper
			Require: key, average rating for a movie
			Require: m, dimension of the bloom filter
			Initialize Bloom F ilter ←new ARRAY(m)
			Bloom F ilter[i] ← 0 for each i = 0,...,m-1
			for each V ALU ES ∈ IN P U T LIST do
			SP LIT T ED V ALU ES ←split(VALUES)
			for each value ∈ SP LIT T ED V ALU ES do
			Bloom F ilter[value] ← 1
			end for
			end for
			write to hdfs(Bloom F ilter)
			*/

            // To be implemented
            int m = 0;
            try {
                m = Integer.parseInt(context.getConfiguration().get("m_" + key.toString()));
            }
            catch (Exception e){
                System.out.println("Error in parsing the input file");
            }
        }
    }

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

/*
-Containins the mapper, the reducer and the driver code.
*/
