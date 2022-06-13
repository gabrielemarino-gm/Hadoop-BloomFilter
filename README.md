# Bloom Filter - Hadoop

Implementation of a Bloom Filter using the Hadoop framework in Java. All the documentation for this project can be found [here](./Documentation)

## How to run the algorithm
Step 1: Create the JAR using Maven,  in the directory named "BloomFilterMapReduce:

` mvn clean package`

Step 2: Send the file JAR to the Hadoop HDFS using the command scp in Linux:

` sudo scp target/BloomFilterMapReduce-1.0-SNAPSHOT.jar hadoop@<IP name node VM>:`

Step 3: Move in the Hadoop user space of the Name Node, using the comand su in Linux

` su -- hadoop`

Step 4: Run the file JAR

` hadoop jar /home/hadoop/BloomFilterMapReduce-1.0-SNAPSHOT.jar it.unipi.hadoop.BloomFilter <inputFile.txt> <outputDirectory> <false positive rate>`

## The input file

We have tested the bloom filter with the *title.ratings* IMDb dataset (available [here](https://datasets.imdbws.com/title.ratings.tsv.gz)).
The dataset is a `.tsv` file, with an header line containing the structure of the data: 

`tconst averageRating   numVotes`

We transform this file in a `.txt` file without the header.

<pre><code>tt0000001	5.7	1882
tt0000002	5.9	250
tt0000003	6.5	1663
tt0000004	5.8	163
tt0000005	6.2	2487
tt0000006	5.2	166
tt0000007	5.4	773
tt0000008	5.4	2024
tt0000009	5.3	194
tt0000010	6.9	6803
tt0000011	5.3	346
tt0000012	7.4	11692
tt0000013	5.7	1801
...             ...     ...
</code></pre>

In the setup phase, in which we compute m for each bloom filter, we also pre-process this dataset, removing the `numVotes` column and rounding the values in the `averageRating` column.
You can find the `data.txt` file (and a reduced version of the same file used for testing purposes `data1.txt`) in [this folder](./Data).

## Outputs
The algorithm generates one output folderr named as indicated in the launch command under the entry `<outputDirectory>`, with inside a file named `part-r-00000` with a bloom filter for each vote. You can chek the folter with the follow comand:

` hadoop fs -ls <outputDirectory>`

For show the output run the comand:

` hadoop fs -cat <outputDirectory>\part-r-00000 | head`

After running on the screen, you will be able to see the false positive rate calculated for each bloom filter.

### Output printed by the application
This is an example of the output printed by the execution of the application, using 0.01 as p value
<pre><code>***** results *****


vote 1 --> false positives: 12905, false positive rate: 0.01036900146636402

vote 2 --> false positives: 12669, false positive rate: 0.01021292444015939

vote 3 --> false positives: 12165, false positive rate: 0.009895795202186593

vote 4 --> false positives: 12056, false positive rate: 0.010017041267119161

vote 5 --> false positives: 11536, false positive rate: 0.010077960925187564

vote 6 --> false positives: 10552, false positive rate: 0.01026928654151217

vote 7 --> false positives: 8754, false positive rate: 0.009993994926512057

vote 8 --> false positives: 8832, false positive rate: 0.010116007262187402

vote 9 --> false positives: 11305, false positive rate: 0.00996977759591226

vote 10 --> false positives: 12287, false positive rate: 0.009981291678411832
</code></pre>

# The *Spark* implementation

You can find our implementation of the bloom filter using Spark in [this repository](https://github.com/PieTempesti98/bloomfilter_spark)
