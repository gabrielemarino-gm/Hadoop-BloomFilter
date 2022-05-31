package it.unipi.hadoop;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;
import it.unipi.hadoop.BloomFilter.IntArrayWritable;


public class BloomFilterMR
{

    public static class BloomFilterMapper extends Mapper<Object, Text, Text, IntArrayWritable>
    {
        // reuse Hadoop's Writable objects
        private final Text outputKey = new Text();
        private final IntArrayWritable outputValue = new IntArrayWritable();
        private Hash h = new MurmurHash();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            //we take k from the configuration
            int k = 0;
            try
            {
                k = Integer.parseInt(context.getConfiguration().get("k"));
            }
            catch (Exception e)
            {
                System.out.println("Error in parsing the input file");
            }
            
            // take the input values from dataset and split them, they are separated by spaces
            String[] inputs = value.toString().split("\t");
            double rate = 0;
            String movie_name = inputs[0];
            
            try
            {
                //we take the rating
                rate = Double.parseDouble(inputs[1]);
            }
            catch(Exception e)
            {
                System.out.println("Error in parsing the input file");
            }

            int index = (int) Math.round((rate));
            // take m from the configuration
            int m = 0;

            try
            {
                m = Integer.parseInt(context.getConfiguration().get("m_" + index));
            }
            catch (Exception e)
            {
                System.out.println("Error in parsing the input file");
            }
            
            //Apply the hash functions
            int[] hashList = new int[k];

            for(int i=0; i<k; i++)
            {
                // pick as seed the index between 0 and k-1 and take the result modulo m, where k is the number of hash functions
                // and m the dimension of the array;
                hashList[i] = (h.hash(movie_name.getBytes(StandardCharsets.UTF_8), movie_name.length(), i)%m + m)%m;
            }
            
            //set the key as the closest integer to the rating
            outputKey.set(String.valueOf(index));
            outputValue.set(hashList);
            context.write(outputKey, outputValue); // <vote, hashList>
        }
    }

    public static class BloomFilterReducer extends Reducer<Text, IntArrayWritable, Text, /*Text*/IntArrayWritable>
    {
        private final IntArrayWritable result = new IntArrayWritable();

        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException
        {

            //take the m value from the configuration based on the key
            int m = 0;
            try 
            {
                m = Integer.parseInt(context.getConfiguration().get("m_" + key.toString()));
            }
            catch (Exception e)
            {
                System.out.println("Error in parsing the input file");
            }
            
            int[] bloomFilter = new int[m];

            // initialize the bloom filter with all zeros
            for(int i=0; i<m; i++)
            {
                bloomFilter[i] = 0;
            }
            
            // take all the hash values arrays in input and for each value write 1 in the Bloom Filter
            // in the position determined by the value
            
            for(IntArrayWritable arr: values)
            {
                
                for(int j=0; j<intArray.length; j++)
                {
                    IntWritable value = new IntWritable();
                    value = arr.get()[j];
                    int pos = value.get();
                    bloomFilter[pos] = 1;
                }
            }

            /*for(IntArrayWritable arr: values)
            {
                IntWritable[] intArray = arr.get();
                for(int j=0; j<intArray.length; j++)
                {
                    IntWritable value = new IntWritable();
                    value = intArray[j];
                    int pos = value.get();
                    bloomFilter[pos] = 1;
                }
            }*/

            /*for(int j = 0; j < values.length; j++)
            {
                IntWritable[] intArray = new IntWritable[values.length]; 
                for(int k=0; k<intArray.length; k++)
                {
                    intArray[k] = values[j][k];
                    IntWritable value = new IntWritable();
                    value = intArray[k];
                    int pos = value.get();
                    bloomFilter[pos] = 1;
                }
            }*/

            result.set(bloomFilter);
            context.write(key, result); // <vote, bloomFilter>
        }
    }

}

/*
-Contains the mapper and the reducer for building up all the bloom filters
*/
