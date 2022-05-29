package it.unipi.hadoop;

import java.io.IOException;
import java.util.*;

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

public class BloomFilterMR
{

    public static class ConstructionMapper extends Mapper<LongWritable, Text, Text, IntWritable>
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
                if(word.toString().startsWith("tt")){
                    continue;
                }
                if (rate == Math.floor(rate) && !Double.isInfinite(rate) && !word.toString().endsWith(".0"))
                {
                    continue;
                }
                word.set(String.valueOf((int) Math.round((rate))));
                context.write(word, one);
            }
        }
    }

    public static class ConstructionReducer extends Reducer<Text, Text, Text, IntWritable>
    {
        // <voto, [1, 1, ..., 1]>
        // Somma
        // Calcolare m

        private final IntWritable result = new IntWritable();

        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
        {
            int n = 0;
            for (final IntWritable val : values)
            {
                n += val.get();
            }

            // Calculate m
            double p = 0.1;
            double m = (int) (-n * (Math.log(p)) / Math.log(2));

            result.set((int) Math.ceil((m))); // Round to the higher int
            context.write(key, result);  // <vote, m>
        }
    }

    public static class BloomFilterMapper1 extends Mapper<LongWritable, Text, Text, Text>
    {
        // reuse Hadoop's Writable objects
        private final Text reducerKey = new Text();
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

        }
    }

    public static class BloomFilterMapper2 extends Mapper<LongWritable, Text, Text, Text>
    {
        // reuse Hadoop's Writable objects
        private final Text reducerKey = new Text();
        //private final TimeSeriesData reducerValue = new TimeSeriesData();

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

        }
    }

    public static class BloomFilterReducer extends Reducer<Text, Text, Text, Text>
    {
        private int windowSize;

        public void setup(Context context) throws IOException, InterruptedException
        {
            // To be implemented
            Configuration conf = context.getConfiguration();

        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
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

        }
    }


}

/*
-Containins the mapper, the reducer and the driver code.
*/
