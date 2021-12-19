import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class Secondary {
    public static class CustomMapper
            extends Mapper<Object, Text, CompositeKey, CompositeValue> {
        private CSVReader csvReader;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header line
            if ( ((LongWritable) key).get() == 0 && value.toString().contains("Year")) return;

            csvReader = new CSVReader(new StringReader(value.toString()));
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                String year = line[0], month = line[2], airlineId = line[7], arrDelayMin = line[37], cancelled = line[41],
                        diverted = line[43];

                List<String> fields = Arrays.asList(year, month, arrDelayMin, cancelled, diverted);
                // Skip processing any row that has empty strings in fields
                if (fields.stream().anyMatch(n -> n.length() == 0)) return;

                Flight f = new Flight(Integer.parseInt(year), Integer.parseInt(month), airlineId, Float.parseFloat(arrDelayMin),
                        (int) Float.parseFloat(cancelled), (int) Float.parseFloat(diverted));
                // Construct composite key (airlineId, month)
                CompositeKey outKey = new CompositeKey(airlineId, f.getMonth());
                // Construct composite value (month, arrDelayMinutes)
                CompositeValue outValue = new CompositeValue(f.getMonth(), f.getArrDelayMinutes());
                // filter out flights that don't meet the conditions
                if (f.getDiverted() == 1 || f.getCancelled() == 1 || f.getYear() != 2008) {
                    return;
                }
                //emit(key, value)
                context.write(outKey, outValue);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            csvReader.close();
        }
    }

    // Only partition by airlineId
    public static class GroupingPartitioner extends Partitioner<CompositeKey, CompositeValue> {
        @Override
        public int getPartition(CompositeKey key, CompositeValue value, int numPartitions) {
            return key.getAirlineId().hashCode() % numPartitions;
        }
    }

    // Only group by airlineId
    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey key1 = (CompositeKey) a, key2 = (CompositeKey) b;
            return key1.getAirlineId().compareTo(key2.getAirlineId());
        }
    }

    // Sort by airlineId first, and then by month
    public static class SortComparator extends WritableComparator {
        public SortComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey key1 = (CompositeKey) a, key2 = (CompositeKey) b;
            if (key1.getAirlineId().equals(key2.getAirlineId())) {
                return Integer.compare(key1.getMonth(), key2.getMonth());
            }
            return key1.getAirlineId().compareTo(key2.getAirlineId());
        }
    }

    public static class CustomReducer
            extends Reducer<CompositeKey, CompositeValue, Text, Text> {

        public void reduce(CompositeKey key, Iterable<CompositeValue> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Local aggregation of minutes delayed for each key
            float numDelayedMinutes = 0f;
            int numFlights = 0, prevMonth = 0, curMonth = 0;
            int[] avgMinPerMonth = new int[12];
            // Calculate local aggregation of flights and minutes delayed
            for(CompositeValue value : values){
                curMonth = value.getMonth();
                if(curMonth != prevMonth && prevMonth > 0){
                    avgMinPerMonth[prevMonth-1] = (int) Math.ceil(numDelayedMinutes / numFlights);
                    prevMonth = curMonth;
                    numDelayedMinutes = 0f;
                    numFlights = 0;
                }
                if (prevMonth == 0) { prevMonth = curMonth;}
                numFlights++;
                numDelayedMinutes += value.getArrDelayedMinutes();
                if (!values.iterator().hasNext()) {
                    avgMinPerMonth[curMonth-1] = (int) Math.ceil(numDelayedMinutes / numFlights);
                }
            }

            Text airlineId = new Text(key.getAirlineId());
            List<String> resultList = new ArrayList<>();
             // Emit result for each month under the same airline
            for (int i = 0; i < avgMinPerMonth.length; i++){
                String result = "(" + (i+1) + ", " + avgMinPerMonth[i] + ")";
                resultList.add(result);
            }
            context.write(new Text(airlineId), new Text(String.join(", ", resultList)));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flight delay");
        job.setJarByClass(Secondary.class);
        job.setMapperClass(CustomMapper.class);
        job.setPartitionerClass(GroupingPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setNumReduceTasks(10);
        job.setReducerClass(CustomReducer.class);
        // Sets output data type for Mapper
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(CompositeValue.class);
        // Sets final output data type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
