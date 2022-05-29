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
        private final Text word = new Text();

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException
        {
            final StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                double rate = Double.parseDouble(String.valueOf(word));
                //if the word starts with "tt" then it's the movieID
                if(word.toString().startsWith("tt")){
                    continue;
                }
                //check if the value of the rate is equal to the smallest integer value closest to it, it's not infinite and the string doesn't contain ".0";
                //if the string contains ".0" we consider it as a rating since it's a double
                if (rate == Math.floor(rate) && !Double.isInfinite(rate) && !word.toString().endsWith(".0"))
                {
                    continue;
                }
                //round the rate to its closest integer value, then convert it to string
                word.set(String.valueOf((int) Math.round((rate))));
                context.write(word, one);
            }
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
            //Compute n summing all the ones in the reducer input list
            int n = 0;
            for (final IntWritable val : values)
            {
                n += val.get();
            }

            // Calculate m
            double p = 0.01;
            double m = (-n * (Math.log(p)) / (Math.log(2)*(Math.log(2))));

            result.set((int) (Math.ceil((m)))); // Round to the higher int
            context.write(key, result);  // <vote, m>
        }
    }
}
