package it.unipi.hadoop;

import java.io.*;
import java.util.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;


public class BloomFilter
{

    public static class ConstructorMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        // Require: DOC, dataset
        // for each WORD in DOC:
        //  if WORD == voto
        //      voto.round()
        //      emit(voto, 1)
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException
        {
            final StringTokenizer itr = new StringTokenizer(value.toString()); // Tokenize the DOC for each word

            while (itr.hasMoreTokens())
            {
                // Check if is a vote or not, if it's not a vote continue for the next iteration.
                String vote = itr.nextToken().toString();

                if(vote.startsWith("tt")) // It's means that this word is the ID of the movie, so discard it.
                    continue;

                double vote_double = Double.parseDouble(vote);
                if (vote_double == Math.floor(vote_double) && !Double.isInfinite(vote_double) && !vote.endsWith(".0")) // If it's integer meas that is a number of total vote, so discard it.
                    continue;


                // Add the token in the context in the format: <key, value> = <vote, 1>
                word.set(String.valueOf((int) Math.round(Double.parseDouble(vote)))); // round the vote and then put inside the Text for the context
                context.write(word, one);
            }
        }
    }

    public static class ConstructorReducer extends Reducer<Text, IntWritable, Text, IntWritable>
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
            double p = 0.01;
            double m = (-n * (Math.log(p)) / (Math.log(2) * Math.log(2)));

            result.set((int) (Math.ceil(m))); // Round m to the higher int

            // Add the result in the context in the format: <key, value> = <vote, m>
            context.write(key, result);
        }
    }

    // Work with the DOC: data.tsv
    public static class BloomFilterMapper1 extends Mapper<Object, Text, Text, IntWritable>
    {
        // Require: HASH_FUNCTIONS, list of hash functions
        // Require: input_split, record in input to the Mapper
        //  Initialize movieName ← input_split.movieName
        //  Initialize avgRating ← roundNextInt(input_split.avgRating)
        //  Initialize hash_values = empty
        //
        //  for each hash_f ∈ HASH_FUNCTIONS do
        //      hashValues[pos] += hashFunction(movieName)
        //      pos++
        //  end for
        //
        //  emit(avgRating, hashValues)

        private static final MurmurHash funHash = new MurmurHash();
        private final Text word = new Text();

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException
        {
            // Tokenize the DOC for each row
            String record = value.toString();
            if (record == null || record.length() == 0)
                return;

            String[] row = record.trim().split("\n");


            for(int r=0; r<row.length; r++)
            {
                int MOVIE = 0;
                int VOTE = 1;

                // Split the row in three part: MovieName, Vote, nVote
                String[] tocken = record.trim().split(" ");

                // I take the vote in a rounded way
                int vote = (int) Math.round(Double.parseDouble(tocken[VOTE])); // Rounded

                // Cast to String from byte[]
                byte[] movieNameByte = tocken[MOVIE].getBytes(StandardCharsets.UTF_8);

                // We need to compute each hash function
                for (int i=0; i<7; i++)
                {
                    funHash.hash(movieNameByte, 10, i);
                }

                // context.write(key, value);  // <vote, m>
            }
        }


    }

    // Work with file that have inside <m>
    public static class BloomFilterMapper2 extends Mapper<Object, Text, Text, IntWritable>
    {

        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException
        {

        }
    }

    public static class BloomFilterReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        // Require: INPUT_LIST, list of hash results computed in the Mapper
        // Require: key, average rating for a movie
        // Require: m, dimension of the bloom filter
        //  Initialize BloomFilter ← new ARRAY(m) [0, 0, ..., 0]
        //
        //  for each VALUES ∈ INPUT_LIST do
        //      SPLITTED_VALUES ← split(VALUES)
        //      for each value ∈ SPLITTED_VALUES do
        //          BloomFilter[value] ← 1
        //      end for
        //  end for
        //
        //  write_to_hdfs(BloomFilter)
    }

    public static void contaVoti()
    {
        int conta_1 = 0;
        int conta_2 = 0;
        int conta_3 = 0;
        int conta_4 = 0;
        int conta_5 = 0;
        int conta_6 = 0;
        int conta_7 = 0;
        int conta_8 = 0;
        int conta_9 = 0;
        int conta_10 = 0;


        String filePath = "D:\\Università\\Magistrale\\Primo anno\\Cloud Computing\\Ciuchino Team\\Data\\titleratings\\data.tsv";
        File file = new File(filePath);
        String riga = " ";

        try(Scanner inputStream = new Scanner(file))
        {
            while(inputStream.hasNext())
            {
                riga = inputStream.next();
                if(riga.equals("t_const") || riga.equals("averageRating") || riga.equals("numVotes") || riga.startsWith("tt"))
                    continue;

                double variable = Double.parseDouble(riga);

                if (variable == Math.floor(variable) && !Double.isInfinite(variable) && !riga.endsWith(".0"))
                {
                    System.out.println("Intero: " + variable);
                    continue;
                }

                // System.out.println("Riga = " + riga);

                if ((int) Math.round(Double.parseDouble(riga)) == 1)
                {
                    conta_1++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 2)
                {
                    conta_2++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 3)
                {
                    conta_3++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 4)
                {
                    conta_4++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 5)
                {
                    conta_5++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 6)
                {
                    conta_6++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 7)
                {
                    conta_7++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 8)
                {
                    conta_8++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 9)
                {
                    conta_9++;
                }
                else if ((int) Math.round(Double.parseDouble(riga)) == 10)
                {
                    conta_10++;
                }
            }

            System.out.println("Film con voto 1: " + conta_1);
            System.out.println("Film con voto 2: " + conta_2);
            System.out.println("Film con voto 3: " + conta_3);
            System.out.println("Film con voto 4: " + conta_4);
            System.out.println("Film con voto 5: " + conta_5);
            System.out.println("Film con voto 6: " + conta_6);
            System.out.println("Film con voto 7: " + conta_7);
            System.out.println("Film con voto 8: " + conta_8);
            System.out.println("Film con voto 9: " + conta_9);
            System.out.println("Film con voto 10: " + conta_10);

            inputStream.close();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    public static void main(final String[] args) throws Exception
    {

        final Configuration conf = new Configuration();

        final Job job = new Job(conf, "bloomFilter");
        job.setJarByClass(BloomFilter.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(ConstructorMapper.class);
        job.setReducerClass(ConstructorReducer.class);

        // MultipleInputs.addInputPath();

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


        // contaVoti();
    }
}
