package exe;

import exc.TopTitleStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
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
// <<< Don't Change

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

        jobCounter.setJarByClass(TopPopularLinks.class);
        jobCounter.waitForCompletion(true);

        Job jobTopLinks = Job.getInstance(conf, "Top Popular Links");
        jobCounter.setOutputKeyClass(IntWritable.class);
        jobCounter.setOutputValueClass(IntWritable.class);

        jobTopLinks.setMapperClass(TopLinksMap.class);
        jobTopLinks.setReducerClass(TopLinksReduce.class);
        jobTopLinks.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobTopLinks, tmpPath);
        FileOutputFormat.setOutputPath(jobTopLinks, new Path(args[1]));

        jobTopLinks.setJarByClass(TopPopularLinks.class);
        return jobTopLinks.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String KV_DELIMITER = ":";
            String VALUE_DELIMITER = "\\s";

            String referrer = value.toString().split(KV_DELIMITER)[0];
            String linkedPages = value.toString().split(KV_DELIMITER)[1];

            for(String page : linkedPages.split(VALUE_DELIMITER)) {
                String cleanPage = page.trim();
                if(cleanPage.length() > 0) {
                    // linked page
                    context.write(new IntWritable(Integer.parseInt(cleanPage)), new IntWritable(1));
                }
            }
            // referrer, which may not have any links - setting to 0
            context.write(new IntWritable(Integer.parseInt(referrer)), new IntWritable(0));
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

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;
        TreeSet<Pair<Integer, Integer>> invertedLinkCount = new TreeSet<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            invertedLinkCount.add(new Pair<>(Integer.parseInt(key.toString()),
                    Integer.parseInt(value.toString())));

            if(invertedLinkCount.size() > N) {
                invertedLinkCount.remove(invertedLinkCount.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Pair<Integer, Integer> item : invertedLinkCount) {
                Integer[] numbers = {item.first, item.second};
                TopPopularLinks.IntArrayWritable val = new IntArrayWritable(numbers);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;
        TreeSet<Pair<Integer, Integer>> invertedLinkCount = new TreeSet<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        // Prune mapper output to capture Top N Popular Links
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            // Loop through mapper values and isolate Top N
            for(IntArrayWritable val : values) {
                IntWritable[] pair = (IntWritable[]) val.toArray();

                Integer link = Integer.parseInt(pair[0].toString());
                Integer count = Integer.parseInt(pair[1].toString());

                invertedLinkCount.add(new Pair<>(link, count));

                if(invertedLinkCount.size() > N) {
                    invertedLinkCount.remove(invertedLinkCount.first());
                }
            }

            // Write output
            for(Pair<Integer, Integer> item : invertedLinkCount) {
                context.write(new IntWritable(item.first), new IntWritable(item.second));
            }
        }
    }
}

// >>> Don't Change
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
// <<< Don't Change