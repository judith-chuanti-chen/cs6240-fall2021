import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    private final static String LETTER_RANGE = "mnopq";
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Map<String, Integer> hashmap;

        public void setup(Context context){
             hashmap = new HashMap();
        }

        public void map(Object key, Text value, Context context) {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                // Only map words that start with 'm', 'n', 'o', 'p', 'q'
                String wordString = word.toString();
                if (LETTER_RANGE.indexOf(wordString.toLowerCase().charAt(0)) > -1){
                    int count = hashmap.getOrDefault(wordString, 0) + 1;
                    hashmap.put(wordString, count);
                }
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String w : hashmap.keySet()){
                context.write(new Text(w), new IntWritable(hashmap.get(w)));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    // Custom partitioner that assigns words that start with 'm', 'n', 'o', 'p', 'q' into different tasks
    public static class RangePartitioner extends Partitioner<Text, IntWritable>{

        public int getPartition(Text text, IntWritable intWritable, int numReduceTasks) {
            char startsWith = text.toString().toLowerCase().charAt(0);
            for(int i = 0; i < LETTER_RANGE.length(); i++){
                if (startsWith == LETTER_RANGE.charAt(i)){
                    return i % numReduceTasks;
                }
            }
            return 0;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setPartitionerClass(RangePartitioner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(5);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

