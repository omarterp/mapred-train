package exd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "Orphan Pages");
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException {
        Path pt = new Path(path);

        try(FileSystem fs = FileSystem.get(pt.toUri(), conf);
            FSDataInputStream file = fs.open(pt);
            BufferedReader buffR = new BufferedReader(new InputStreamReader(file))) {

    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            "2: 4567 7583 9382 832",
                    "4567: 32 847 921 2827",
                    "23: 324 73 8387 382"
        };

        // Mapper Code
            List<Integer> linkedPage = new ArrayList<>();
            for(String line : lines) {
                String referrer = line.split(":")[0];
                String linkedPages = line.split(":")[1];
                System.out.println(linkedPages);

                for(String page : linkedPages.split("\\s")) {
                    String cleanPage = page.trim();
                    if(cleanPage.length() > 0)
                        linkedPage.add(Integer.parseInt(cleanPage));
                }

                System.out.println(referrer + " : " + linkedPage);
            }
            // Mapper Code


        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
        }
    }
}