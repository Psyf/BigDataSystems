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
import java.io.IOException;
import java.util.*;

public class Recommend {

    // JOB 1
    // Mapper not doing much, just emitting (User, X, Item Y)
    public static class MapperCOMC extends Mapper<Object, Text, Text, IntWritable> {
        Text user_id = new Text();
        IntWritable item_id = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = (value.toString()).split(",");
            user_id.set(line[0]);
            item_id.set(Integer.parseInt(line[1]));
            context.write(user_id, item_id);
        }
    }

    // JOB 1
    // Reducer Sets up a Co-Occurrence "Matrix" - really a nested hash map since we expect it to be relatively sparse
    // Possible Improvement: If we expect the User Vector to be more sparse, we can do that here instead.
    // Spills the Matrix onto disk for next MR job to take
    public static class ReducerCOMC extends Reducer<Text, IntWritable, IntWritable, Text> {
        IntWritable item_id = new IntWritable();
        Text row = new Text();
        Map<Integer, Map<Integer, Integer>> COM;

        public void setup(Context context) {
            COM = new HashMap<>();
        }

        public void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {

            ArrayList<Integer> items = new ArrayList<>();
            for (IntWritable item : value) { items.add(item.get()); }

            for (Integer item_1 : items) {
                for (Integer item_2 : items) {
                    if (item_1 != item_2) {
                        if (COM.containsKey(item_1)) {
                            if (COM.get(item_1).containsKey(item_2)) {
                                COM.get(item_1).put(item_2, COM.get(item_1).get(item_2)+1);
                            } else {
                                COM.get(item_1).put(item_2, 1);
                            }
                        } else {
                            COM.put(item_1, new HashMap<>());
                            COM.get(item_1).put(item_2, 1);
                        }
                    }
                     
                }
            }
        }

        // Explicit serializing.
        // Next job needs to know the "protocol" to deserialize
        public void cleanup(Context context) throws IOException, InterruptedException {
            String row_as_str;
            for (Integer item_1 : COM.keySet()) {
                row_as_str = "";
                for (Integer item_2 : COM.get(item_1).keySet()) {
                    row_as_str += String.valueOf(item_2);
                    row_as_str += ",";
                    row_as_str += String.valueOf(COM.get(item_1).get(item_2));
                    row_as_str += "/";
                }

                //System.out.print(item_1);
                //System.out.print("\t");
                //System.out.println(row_as_str);

                item_id.set(item_1);
                row.set(row_as_str.substring(0, row_as_str.length()-1));
                context.write(item_id, row);
            }
        }
    }

    // JOB 2
    // Lightweight Mapper again!
    public static class MapperScore extends Mapper<Object, Text, Text, Text> {
        Text user_id = new Text();
        Text item_score = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = (value.toString()).split(",");

            //System.out.println(value.toString());

            user_id.set(line[0]);
            item_score.set(line[1] + '_' + line[2]);
            context.write(user_id, item_score);
        }
    }

    // JOB 2
    // Setup first recreates the Co-Occurrence Matrix from previous Job
    // Reducer:
    // For each user X
    //      For each item Y _not rated_ by X:
    //          Multiply Row Y with User X vector to find score.
    //          Emit to disk.
    public static class ReducerScore extends Reducer<Text, Text, Text, Text> {
        Text item_score = new Text();
        Map<Integer, Map<Integer, Integer>> COM;

        public void setup(Context context) {
            COM = new HashMap<>();
            Configuration conf = context.getConfiguration();
            StringTokenizer row_itr = new StringTokenizer(conf.get("mtx"), "\n");
            while (row_itr.hasMoreElements()) {
                String[] row = (row_itr.nextToken()).split("\t");
                //System.out.println(row[0]);
                //System.out.println(row[1]);
                COM.put(Integer.parseInt(row[0]), new HashMap<>());

                String[] entries = row[1].split("/");

                for (String entry : entries) {
                    String[] col_cell = entry.split(",");
                    COM.get(Integer.parseInt(row[0])).put(Integer.parseInt(col_cell[0]), Integer.parseInt(col_cell[1]));
                }
            }
        }

        public void reduce(Text key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            Map<Integer, Double> user_ratings = new HashMap<>();
            for (Text entry : value) {
                String[] item_score = entry.toString().split("_");
                user_ratings.put(Integer.parseInt(item_score[0]), Double.parseDouble(item_score[1]));
            }

            Double score;

            for (Integer item_1 : COM.keySet()) {
                score = 0.0;
                if (!user_ratings.containsKey(item_1)) {
                    for (Integer item_rated : user_ratings.keySet()) {
                        if (COM.get(item_1).containsKey(item_rated)) {
                            score += user_ratings.get(item_rated) * COM.get(item_1).get(item_rated);
                        }
                    }
                    item_score.set(item_1+","+score);
                    context.write(key, item_score);
                }
            }
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job comc = Job.getInstance(conf, "co occurrence matrix calculator");
        comc.setJarByClass(Recommend.class);
        comc.setMapperClass(MapperCOMC.class);
        comc.setReducerClass(ReducerCOMC.class);
        comc.setMapOutputKeyClass(Text.class);
        comc.setMapOutputValueClass(IntWritable.class);
        comc.setOutputKeyClass(IntWritable.class);
        comc.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(comc, new Path(args[0]));
        FileOutputFormat.setOutputPath(comc, new Path("CC"));

        if (comc.waitForCompletion(true)) {
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(new Path("CC/"))) {
                Path path = new Path("CC/part-r-00000");
                FSDataInputStream in = fs.open(path);
                String mtx = IOUtils.toString(in, "UTF-8");
                conf.set("mtx", mtx);
                fs.delete(new Path("CC/"), true);
            }
        }

        Job score = Job.getInstance(conf, "score");
        score.setJarByClass(Recommend.class);
        score.setMapperClass(MapperScore.class);
        score.setReducerClass(ReducerScore.class);
        score.setMapOutputKeyClass(Text.class);
        score.setMapOutputValueClass(Text.class);
        score.setOutputKeyClass(Text.class);
        score.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(score, new Path(args[0]));
        FileOutputFormat.setOutputPath(score, new Path(args[1]));

        if (score.waitForCompletion(true)) {
            System.exit(0);
        }
    }
}
