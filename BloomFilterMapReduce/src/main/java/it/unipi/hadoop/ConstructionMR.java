
package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class ConstructionMR {
    public static class ConstructionMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        // Require: DOC, dataset
        // for each WORD in DOC:
        //  if WORD == voto
        //      voto.round()
        //      emit(voto, 1)

        private final static IntWritable one = new IntWritable(1);
        private final Text keyWord = new Text();

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
        // <voto, [1, 1, ..., 1]>
        // Somma
        // Calcolare m
        private final IntWritable result = new IntWritable();

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
