import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

public class TopKPairs {
    private static final String DELIMITER = ",";
    public static class CustomMapper extends Mapper<Object, Text, IntWritable, Pair> {
        private CSVReader csvReader;
        private Map<String, Integer> counter;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            counter = new HashMap<>();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException {
            if ( ((LongWritable) key).get() == 0 && value.toString().contains("rental_id")) return;
            csvReader = new CSVReader(new StringReader(value.toString()));
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                String endRentalDateTime = line[3];
                int startStationId = line[7].length() > 0 ? Integer.parseInt(line[7]) : -1,
                        endStationId = line[4].length() > 0 ? Double.valueOf(line[4]).intValue() : -1;
                if (endRentalDateTime.length() < 10 || startStationId < 0 || endStationId < 0) {
                    continue;
                }
                int year, month;
                try {
                    String date = endRentalDateTime.split(" ")[0];
                    month = LocalDate.parse(date).getMonthValue();
                    year = LocalDate.parse(date).getYear();
                } catch (DateTimeParseException e) {
//                    e.printStackTrace();
                    continue;
                }
                if (year == 2019 && month > 0) {
                    String counterKey = month + DELIMITER + startStationId + DELIMITER + endStationId;
                    counter.put(counterKey, counter.getOrDefault(counterKey, 0)+1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            csvReader.close();
            for (String key : counter.keySet()) {
                String[] keys = key.split(DELIMITER);
                int month = Integer.parseInt(keys[0]), startStationId = Integer.parseInt(keys[1]),
                        endStationId = Integer.parseInt(keys[2]);
                context.write(new IntWritable(month),
                        new Pair(startStationId, endStationId, counter.get(key)));
            }
        }
    }

    public static class RangePartitioner extends Partitioner<IntWritable, Pair> {
        @Override
        public int getPartition(IntWritable key, Pair value, int i) {
            int month = key.get();
            return (month-1) % i;
        }
    }
    public static class PlainReducer extends Reducer<IntWritable, Pair, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
            int month = key.get();
            for (Pair pair : values) {
                String outKey = String.format("(month=%s, start=%s, end=%s)", month, pair.getStartStationId(),
                        pair.getEndStationId());
                context.write(new Text(outKey), new IntWritable(pair.getCount()));
            }
        }
    }


    public static class TopKReducer
            extends Reducer<IntWritable, Pair, Text, IntWritable> {
        private Queue<Pair> minHeap = new PriorityQueue<>();
        // in case one mapper can't read all entries of the same month
        private Map<String, Integer> counter = new HashMap<>();
        private static final int K = 10;
        public void reduce(IntWritable key, Iterable<Pair> values, Context context)
                throws IOException, InterruptedException {
            int month = key.get();
            // aggregate again
            for (Pair pair: values) {
                String counterKey = pair.getStartStationId() + DELIMITER + pair.getEndStationId();
                counter.put(counterKey, counter.getOrDefault(counterKey, 0)+pair.getCount());
            }
            // populate minheap
            for (String counterKey : counter.keySet()) {
                String[] keyValues = counterKey.split(DELIMITER);
                int startStationId = Integer.parseInt(keyValues[0]), endStationId = Integer.parseInt(keyValues[1]);
                Pair pair = new Pair(startStationId, endStationId, counter.get(counterKey));
                if (minHeap.size() < K) {minHeap.add(pair);}
                else if (minHeap.peek().compareTo(pair) < 0) {
                    minHeap.poll();
                    minHeap.add(pair);
                }
            }
            // get top k
            Pair[] topK = new Pair[K];
            int i = K-1;
            while (minHeap.size() > 0) {
                topK[i--] = minHeap.poll();
            }
            // emit results
            for (Pair pair : topK) {
                String outKey = String.format("(month=%s, start=%s, end=%s)", month, pair.getStartStationId(),
                        pair.getEndStationId());
                context.write(new Text(outKey), new IntWritable(pair.getCount()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top k pairs");
        job.setJarByClass(TopKPairs.class);
        job.setMapperClass(CustomMapper.class);
        job.setPartitionerClass(RangePartitioner.class);
        job.setReducerClass(TopKReducer.class);
        job.setNumReduceTasks(12);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Pair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
