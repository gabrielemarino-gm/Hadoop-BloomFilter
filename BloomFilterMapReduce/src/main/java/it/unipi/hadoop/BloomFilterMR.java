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
import it.unipi.hadoop.BloomFilter.IntArrayWritable;

public class BloomFilterMR
{

    public static class BloomFilterMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable>
    {
        // reuse Hadoop's Writable objects
        private final Text outputKey = new Text();
        private final IntArrayWritable outputValue = new IntArrayWritable();
        private final int k = 7;
        private Hash h = new MurmurHash();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            //take the input values and split them, they are separated by spaces
            String[] inputs = value.toString().split(" ");

            //if number of input values is less than expected do nothing
            if(inputs.length < 3){
                return;
            }
            double rate = 0;
            String movie_name = inputs[0];
            String rating = "";
            try{
                //we first take the rating as a string
                rating = inputs[1];
                //then we take it also as a double
                rate = Double.parseDouble(inputs[1]);
            }
            catch(Exception e){
                System.out.println("Error in parsing the input file");
            }

            //check if the value of the rate is equal to the smallest integer value closest to it, it's not infinite and the string doesn't contain ".0";
            //if the string contains ".0" we consider it as a rating since it's a double
            if (rate == Math.floor(rate) && !Double.isInfinite(rate) && !rating.endsWith(".0"))
            {
                System.out.println("Error in parsing the input file");
            }
            //Take m from the configuration
            int m = 0;
            try {
                m = Integer.parseInt(context.getConfiguration().get("m_" + rate));
            }
            catch (Exception e){
                System.out.println("Error in parsing the input file");
            }
            //Apply the hash functions
            IntWritable hashList[] = new IntWritable[k];
            for(int i = 0; i < k; i++) {
                //Pick as seed a random number between 0 and k modulo m, where k is the number of hash functions and m the dimension of the array;
                int seed = (int)(Math.random()*(k-1))%m;
                hashList[i] = new IntWritable(h.hash(movie_name.getBytes(StandardCharsets.UTF_8), movie_name.length(), seed));
            }
            //set the key as the closest integer to the rating
            outputKey.set(String.valueOf((int) Math.round((rate))));

            outputValue.set(hashList);
            context.write(outputKey, outputValue);

        }
    }

    public static class BloomFilterReducer extends Reducer<Text, IntWritable, Text, Text>
    {

        public void setup(Context context) throws IOException, InterruptedException
        {
            // To be implemented
            Configuration conf = context.getConfiguration();

        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            /*
			Algorithm 2 REDUCER
			Require: INPUT LIST , list of hash results computed in the Mapper
			Require: key, average rating for a movie
			Require: m, dimension of the bloom filter
			Initialize Bloom Filter ←new ARRAY(m)
			Bloom Filter[i] ← 0 for each i = 0,...,m-1
			for each V ALU ES ∈ IN P U T LIST do
			SP LIT T ED V ALU ES ←split(VALUES)
			for each value ∈ SP LIT T ED V ALU ES do
			Bloom Filter[value] ← 1
			end for
			end for
			write to hdfs(Bloom Filter)
			*/

            int m = 0;
            try {
                m = Integer.parseInt(context.getConfiguration().get("m_" + key.toString()));
            }
            catch (Exception e){
                System.out.println("Error in parsing the input file");
            }
        }
    }




}

/*
-Containins the mapper, the reducer and the driver code.
*/
