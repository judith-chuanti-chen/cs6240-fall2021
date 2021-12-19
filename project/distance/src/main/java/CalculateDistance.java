import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalculateDistance {
    public static class CustomMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private CSVReader csvReader;
        private List<Integer> stations;
        private Map<Integer, float[]> map;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            stations = new ArrayList<>();
            map = new HashMap<>();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (((LongWritable) key).get() == 0 && value.toString().contains("station_id")) return;
            csvReader = new CSVReader(new StringReader(value.toString()));
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                String stationIdStr = line[0].replace(" ", ""),
                        longitude = line[2].replace(" ", ""),
                        latitude = line[3].replace(" ", "");
                if (stationIdStr.length() == 0 || longitude.length() == 0 || latitude.length() == 0) { continue; }
                int stationId = Integer.parseInt(stationIdStr);
                Float x = Float.parseFloat(longitude), y = Float.parseFloat(latitude);
                stations.add(stationId);
                float[] location = {x, y};
                map.put(stationId, location);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            csvReader.close();
            // calculate distance between each pair
            for (Integer a : stations) {
                for (Integer b : stations) {
                    if (a == b) continue;
//                    float squareSum = (float) (Math.pow(map.get(a)[0] - map.get(b)[0], 2) + Math.pow(map.get(a)[1] - map.get(b)[1], 2));
                    float distance = calculateDistance(map.get(a)[0], map.get(a)[1], map.get(b)[0], map.get(b)[1]);
                    String outKey = "(" + a + "," + b + ")";
                    context.write(new Text(outKey), new FloatWritable(distance));
                }
            }
        }
    }

    public static float calculateDistance(float lon1, float lat1, float lon2, float lat2) {
        double AVERAGE_RADIUS_OF_EARTH_METERS = 6371000;
        double lngDistance = Math.toRadians(lon1 - lon2);
        double latDistance = Math.toRadians(lat1 - lat2);

        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return Math.round(AVERAGE_RADIUS_OF_EARTH_METERS * c);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calculate distance");
        job.setJarByClass(CalculateDistance.class);
        job.setMapperClass(CustomMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
