import java.io.IOException;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.shaded.org.apache.commons.io.IOUtils;


public class TopkCommonWords {
    public static class Mapper_1
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String stopWords = conf.get("stopwords");
            Set<String> stopWordSet = new HashSet<>();
            StringTokenizer itr_sw = new StringTokenizer(stopWords);

            while (itr_sw.hasMoreTokens()) {
                stopWordSet.add(itr_sw.nextToken());
            }

            String candidate;

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                candidate = itr.nextToken();
                if (!stopWordSet.contains(candidate)) {
                    word.set(candidate);
                    context.write(word, one);
                }
            }
        }
    }

    public static class Combiner_1
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            context.write(key, result);
        }
    }

    public static class Reducer_1 extends Reducer<Text, IntWritable, IntWritable, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(result, key);
        }
    }

    public static class Mapper_2
            extends Mapper<Object, Text, Text, IntWritable>{

        private Text word = new Text();
        private IntWritable count = new IntWritable();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] pair = (value.toString()).split("\t");
            word.set(pair[1]);
            count.set(Integer.parseInt(pair[0]));
            context.write(word, count);
        }
    }

    public static class Reducer_2
            extends Reducer<Text,IntWritable,IntWritable, Text> {

        class Pair implements Comparable<Pair> {
            public String word;
            public Integer count;

            @Override
            public int compareTo(Pair p) {
                if (!p.count.equals(this.count)) {
                    return this.count - p.count;
                } else {
                    return p.word.compareTo(this.word);
                }
            }

            public Pair(String word, Integer count) {
                this.word = word;
                this.count = count;
            }
        }

        private Text word = new Text();
        private IntWritable result = new IntWritable();

        // CONFIGURE N according to need
        int N = 20;
        PriorityQueue<Pair> pq = null;
        
        protected void setup(Context context)
                throws IOException, InterruptedException {
                pq = new PriorityQueue<>(N);
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            List<Integer> values_as_int = new ArrayList<>();
            for (IntWritable item : values) {
                values_as_int.add(item.get());
            }

            int min_count;
            if (values_as_int.size() == 2) {
                min_count = Math.min(values_as_int.get(0), values_as_int.get(1));
                if (pq.size() < N) {
                    pq.add(new Pair(key.toString(), min_count));
                } else {
                    if (pq.peek().count < min_count) {
                        pq.poll();
                        pq.add(new Pair(key.toString(), min_count));
                    }
                }
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            Stack<Pair> stack = new Stack<>();

            for (int i=0; i<N; i++) {
                stack.push(pq.poll());
            }

            Pair item;
            for (int i=0; i<N; i++) {
                item = stack.pop();
                result.set(item.count);
                word.set(item.word);
                context.write(result, word);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(conf);
        Path path_of_file = new Path(args[2]);
        if (!fileSystem.exists(path_of_file)) {
            System.out.println("File not exists");
            return;
        }
        FSDataInputStream in = fileSystem.open(path_of_file);

        String stopwords = IOUtils.toString(in, "UTF-8");
        conf.set("stopwords", stopwords);
        in.close();


        // Job 1 for File 1
        Job job_1 = Job.getInstance(conf, "word count f1");
        job_1.setJarByClass(TopkCommonWords.class);
        job_1.setJarByClass(TopkCommonWords.class);
        job_1.setMapperClass(Mapper_1.class);
        job_1.setCombinerClass(Combiner_1.class);
        job_1.setReducerClass(Reducer_1.class);
        job_1.setOutputKeyClass(Text.class);
        job_1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_1, new Path("TempOutput1"));

        // Job 2 for File 2
        Job job_2 = Job.getInstance(conf, "word count f2");
        job_2.setJarByClass(TopkCommonWords.class);
        job_2.setMapperClass(Mapper_1.class);
        job_2.setCombinerClass(Combiner_1.class);
        job_2.setReducerClass(Reducer_1.class);
        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job_2, new Path("TempOutput2"));

        if (job_1.waitForCompletion(true) && job_2.waitForCompletion(true)) {
            // Job 3 for Sorting
            Job job_3 = Job.getInstance(conf, "sort");
            job_3.setJarByClass(TopkCommonWords.class);
            job_3.setMapperClass(Mapper_2.class);
            job_3.setReducerClass(Reducer_2.class);
            job_3.setOutputKeyClass(Text.class);
            job_3.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job_3, new Path("TempOutput1/part-r-00000"));
            FileInputFormat.addInputPath(job_3, new Path("TempOutput2/part-r-00000"));
            FileOutputFormat.setOutputPath(job_3, new Path(args[3]));
            job_3.waitForCompletion(true);
        }

        if (fileSystem.exists(new Path("TempOutput1/"))) {
            fileSystem.delete(new Path("TempOutput1/"), true);
        }

        if (fileSystem.exists(new Path("TempOutput2"))) {
            fileSystem.delete(new Path("TempOutput2"), true);
        }

        fileSystem.close();

        System.exit(1);
    } 
}
