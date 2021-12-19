import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDelay {
    // Global counters
    public enum CustomCounters {
        DELAY_MIN_SUM, DELAY_FLIGHT_SUM
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, ArrayWritable> {
        private Map<String, List<String>> hashmap;


        public void setup(Context context){
            hashmap = new HashMap();
        }

        public void map(Object key, Text value, Context context) throws IOException {
            // Skip the header line
            if ( ((LongWritable) key).get() == 0 && value.toString().contains("Year")) return;

            CSVReader csvReader = new CSVReader(new StringReader(value.toString()));
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                String year = line[0], month = line[2], flightDate = line[5], origin = line[11], dest = line[17],
                        deptTime = line[24], arrTime = line[35], arrDelayMin = line[37], cancelled = line[41],
                        diverted = line[43];

                List<String> fields = Arrays.asList(year, month, flightDate, origin, dest, deptTime,
                        arrTime, arrDelayMin, cancelled, diverted);
                // Skip processing any row that has empty strings in fields
                if (fields.stream().anyMatch(n -> n.length() == 0)) return;

                Flight f = new Flight(Integer.parseInt(year), Integer.parseInt(month), flightDate, origin, dest,
                        Integer.parseInt(deptTime), Integer.parseInt(arrTime), Float.parseFloat(arrDelayMin),
                        (int) Float.parseFloat(cancelled), (int) Float.parseFloat(diverted));
                String outKey = f.getFlightDate() + ",";

                LocalDate startDate = LocalDate.of(2007, Month.JUNE, 1);
                LocalDate endDate = LocalDate.of(2008, Month.MAY, 31);
                LocalDate date = LocalDate.parse(f.getFlightDate());

                // filter out flights that don't meet the conditions
                if (f.getDiverted() == 1 || f.getCancelled() == 1 || date.compareTo(startDate) < 0
                        || date.compareTo(endDate) > 0) {
                    return;
                }

                // Sets leg to 1 or 2 based on origin and destination;
                // leg 1 outKey = [date, destination];
                // leg 2 outKey = [date, origin];
                if (f.getOrigin().equals("ORD") && !f.getDest().equals("JFK")) {
                    f.setLeg(1);
                    outKey += f.getDest();
                } else if (!f.getOrigin().equals("ORD") && f.getDest().equals("JFK")) {
                    f.setLeg(2);
                    outKey += f.getOrigin();
                } else {
                    return;
                }
                // Local aggregation
                hashmap.putIfAbsent(outKey, new ArrayList<>());
                hashmap.get(outKey).add(f.toString());

            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String key : hashmap.keySet()){
                // Value type is TextArrayWritable which is essentially an array of Text objects
                context.write(new Text(key), new TextArrayWritable(hashmap.get(key).stream().toArray(String[]::new)));
            }

        }
    }

    public static class CustomReducer
            extends Reducer<Text, TextArrayWritable, Text, Text> {
        private static final String DELAY_MIN_SUM = "total_minutes_delayed";
        private static final String DELAY_FLIGHT_SUM = "total_flights_delayed";

        public void reduce(Text key, Iterable<TextArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<Flight> leg1 = new ArrayList<>();
            List<Flight> leg2 = new ArrayList<>();

            // Local aggregation of flights and minutes delayed for each key
            int numDelayedFlight = 0;
            float numDelayedMinutes = 0;

            // Populate lists of leg1 and leg2 flights
            for (TextArrayWritable value : values){
                for (String s : value.toStrings()) {
                    Flight f = new Flight(s.split(","));
                    if (f.getLeg() == 1) {
                        leg1.add(f);
                    } else if (f.getLeg() == 2) {
                        leg2.add(f);
                    }
                }
            }

            // Calculate local aggregation of flights and minutes delayed
            for (Flight l1: leg1) {
                for (Flight l2: leg2) {
                    if (l1.getArrTime() >= l2.getDepTime()) continue;
                    float delay = l1.getArrDelayMinutes() + l2.getArrDelayMinutes();
                    numDelayedFlight += 1;
                    numDelayedMinutes += delay;
                }
            }
            // Increment global aggregation of flights and minutes delayed
            context.getCounter(CustomCounters.DELAY_FLIGHT_SUM).increment(numDelayedFlight);
            context.getCounter(CustomCounters.DELAY_MIN_SUM).increment((long)numDelayedMinutes);
            String result = DELAY_FLIGHT_SUM + "=" + numDelayedFlight + ", " + DELAY_MIN_SUM + "=" + numDelayedMinutes;
            // Emit results (optional)
//            context.write(key, new Text(result));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flight delay");
        job.setJarByClass(FlightDelay.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(10);
        job.setReducerClass(CustomReducer.class);
        // Sets output data type for Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);
        // Sets final output data type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Wait for job to complete before calling counters
        job.waitForCompletion(true);

        Counters counters = job.getCounters();
        float totalMinutesDelayed = counters.findCounter(CustomCounters.DELAY_MIN_SUM).getValue();
        long totalFlightsDelayed = counters.findCounter(CustomCounters.DELAY_FLIGHT_SUM).getValue();

        System.out.println("total_minutes_delayed=" + totalMinutesDelayed);
        System.out.println("total_flights_delayed=" + totalFlightsDelayed);
        System.out.println("avg_delay_in_minutes=" + totalMinutesDelayed / totalFlightsDelayed);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


