package exf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    // Read file from hdfs - Pull popularity League
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt = new Path(path);

        try(FileSystem fs = FileSystem.get(pt.toUri(), conf);
            FSDataInputStream file = fs.open(pt);
            BufferedReader buffR = new BufferedReader(new InputStreamReader(file))) {

            StringBuilder everything = new StringBuilder();
            String line;
            while((line = buffR.readLine()) != null) {
                everything.append(line);
                everything.append(System.lineSeparator());
            }
            return everything.toString();
        }
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobCounter = Job.getInstance(conf, "Link Counter");
        jobCounter.setOutputKeyClass(IntWritable.class);
        jobCounter.setOutputValueClass(IntWritable.class);

        jobCounter.setMapperClass(LinkCountMap.class);
        jobCounter.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobCounter, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobCounter, tmpPath);

        jobCounter.setJarByClass(PopularityLeague.class);
        jobCounter.waitForCompletion(true);

        Job jobPageRank = Job.getInstance(conf, "Top Popular Links");
        jobCounter.setOutputKeyClass(IntWritable.class);
        jobCounter.setOutputValueClass(IntWritable.class);

        jobPageRank.setMapperClass(RankLinks.class);
        jobPageRank.setReducerClass(TopLinksReduce.class);
        jobPageRank.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobPageRank, tmpPath);
        FileOutputFormat.setOutputPath(jobPageRank, new Path(args[1]));

        jobPageRank.setJarByClass(PopularityLeague.class);
        return jobPageRank.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        List<String> linksLeague;

        // Pull Popularity League - this set will be drive the ranking
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            linksLeague = Arrays.asList(readHDFSFile("league", context.getConfiguration()).split(System.lineSeparator()));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String KV_DELIMITER = ":";
            String VALUE_DELIMITER = "\\s";

            String linkedPages = value.toString().split(KV_DELIMITER)[1];

            // count list that exist in the links league
            for(String page : linkedPages.split(VALUE_DELIMITER)) {
                String cleanPage = page.trim();
                if(cleanPage.length() > 0 & linksLeague.contains(cleanPage)) {
                    context.write(new IntWritable(Integer.parseInt(cleanPage)), new IntWritable(1));
                }
            }

            // Default links league with zero count to include in results, even if not found
            for(String link : linksLeague){
                context.write(new IntWritable(Integer.parseInt(link)), new IntWritable(0));
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int linkedTotal = 0;

            for(IntWritable hit : values) {
                linkedTotal += hit.get();
            }

            context.write(key, new IntWritable(linkedTotal));
        }
    }

    public static class RankLinks extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        TreeSet<Pair<Integer, Integer>> invertedLinkCount = new TreeSet<>();

        // Populated ordered Set
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            invertedLinkCount.add(new Pair<>(Integer.parseInt(key.toString()),
                    Integer.parseInt(value.toString())));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Pair<Integer, Integer> item : invertedLinkCount) {
                Integer[] numbers = {item.first, item.second};
                PopularityLeague.IntArrayWritable val = new IntArrayWritable(numbers);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        TreeSet<Pair<Integer, Integer>> invertedLinkCount = new TreeSet<>();

        // Finalize Ranking
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            // Loop through mapper values and isolate Top N
            for(IntArrayWritable val : values) {
                IntWritable[] pair = (IntWritable[]) val.toArray();

                Integer link = Integer.parseInt(pair[0].toString());
                Integer count = Integer.parseInt(pair[1].toString());

                invertedLinkCount.add(new Pair<>(link, count));
            }

            // Write output - rank based on order - lastVal used to track ties and index used to increment rank
            int rank = 0;
            int index = 0;
            int lastVal = 0;

            for(Pair<Integer, Integer> item : invertedLinkCount) {
                if(item.second == lastVal || index == 0) {
                    rank = rank;
                } else {
                    rank += index;
                }
                context.write(new IntWritable(item.first), new IntWritable(rank));
                index++;
            }
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}