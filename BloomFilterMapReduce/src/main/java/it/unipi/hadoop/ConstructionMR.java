
package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class ConstructionMR {
    public static class ConstructionMapper extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one = new IntWritable(1);
        private final Text keyWord = new Text();

        /**
         * Map function that takes in input a record of the dataset and retrieves the id of the movie
         * and the rating of the movie; rounds the rating to its closest integer value
         * @param  key      the key of the input of the map function
         * @param  value    < movie_id, rating, number of votes>
         * @param  context  the context which includes the data configuration of the job
         * @return          < rating, 1>
         */
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException
        {
            String[] inputs = value.toString().split("\t");
            double rate = 0;
            try
            {
                //we take the rating
                rate = Double.parseDouble(inputs[1]);
            }
            catch(Exception e)
            {
                System.out.println("Error in parsing the input file");
            }
            keyWord.set(String.valueOf((int) Math.round((rate))));
            context.write(keyWord, one);
        }
    }


    public static class ConstructionReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private final IntWritable result = new IntWritable();

        /**
         * Reduce function that takes in input a rating with the corresponding list of occurrencies; computes
         * the number of total occurencies per key and then computes m, the number of bits for the bloom
         * filter for the rating given by the key
         * @param  key      the key of the input of the reduce function, which is the rating
         * @param  values   arrays containing the list of ones which represents the occurrencies of the key
         * @param  context  the context which includes the data configuration of the job
         * @return          < rating, number of bits of the bloom filter for that rating>
         */
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
        {
            double p = 0;
            try
            {
                p = Double.parseDouble(context.getConfiguration().get("p"));
            }
            catch (Exception e)
            {
                System.out.println("Error in parsing the input file");
            }
            
            //Compute n summing all the ones in the reducer input list
            int n = 0;
            for (final IntWritable val : values)
            {
                n += val.get();
            }

            // Calculate m
            double m = (-n * (Math.log(p)) / (Math.log(2)*(Math.log(2))));
            result.set((int) (Math.ceil((m)))); // Round to the higher int
            context.write(key, result);  // <vote, m>
        }
    }
}

/*
-Contains the mapper and the reducer for the bloom filter parameters construction
*/
