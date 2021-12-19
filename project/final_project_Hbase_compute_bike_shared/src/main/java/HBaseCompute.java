import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class HBaseCompute {
  public static class DurationMapper extends TableMapper<Text, FloatArrayWritable> {
    private Map<String, List<Float>> hashMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      hashMap = new HashMap<>();
    }

    @Override
    protected void map(ImmutableBytesWritable rowKey, Result value, Context context) throws IOException, InterruptedException {
      String startStationId = new String(value.getValue(Bytes.toBytes(BikeDataConstant.COL_FAMILY),
          Bytes.toBytes(BikeDataConstant.START_STATION_ID)));
      String endStationId = new String(value.getValue(Bytes.toBytes(BikeDataConstant.COL_FAMILY),
          Bytes.toBytes(BikeDataConstant.END_STATION_ID)));
      String duration = new String(value.getValue(Bytes.toBytes(BikeDataConstant.COL_FAMILY),
          Bytes.toBytes(BikeDataConstant.DURATION)));
      String outKey = "(" + startStationId + "," + endStationId + ")";
//      String outValue = startStationId + "," + endStationId + "," + duration;
//      context.write(new Text(outKey), new Text(outValue));

      hashMap.putIfAbsent(outKey, new ArrayList<>());
      hashMap.get(outKey).add(Float.parseFloat(duration));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (String key : hashMap.keySet()) {
        context.write(new Text(key), new FloatArrayWritable(hashMap.get(key)));
      }
      super.cleanup(context);
    }
  }

  public static class CustomReducer extends Reducer<Text, FloatArrayWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<FloatArrayWritable> values, Context context) throws IOException, InterruptedException {
      float durationSum = 0;
      int durationCount = 0;

      for (FloatArrayWritable durations : values) {
        for (Writable duration : durations.get()) {
          durationSum += ((FloatWritable) duration).get();
          durationCount += 1;
        }
      }
      float avgDuration = durationSum / durationCount;
      context.write(key, new Text(String.valueOf(avgDuration)));
    }
  }

  public static Scan getFilteredScan(){
    String startRowKeyPrefix = "16:00:00";
    String stopRowKeyPrefix = "19:00:01";
    Scan scan = new Scan()
        .withStartRow(Bytes.toBytes(startRowKeyPrefix))
        .withStopRow(Bytes.toBytes(stopRowKeyPrefix));
    // Higher caching values enable faster scanners but use more memory
//    scan.setCaching(10000);
    // don't set to true for MR jobs
    scan.setCacheBlocks(false);
    return scan;
  }

  public static void main(String[] args) throws Exception {
    HBaseService hBaseService = HBaseService.createHBaseService();
    Configuration conf = hBaseService.getConf();
    Job job = Job.getInstance(conf,"HBaseCompute");
    job.setJarByClass(HBaseCompute.class);
    Scan scan = getFilteredScan();
    TableMapReduceUtil.initTableMapperJob(
        BikeDataConstant.TABLE_NAME,        // input table
        scan,               // Scan instance to control CF and attribute selection
        DurationMapper.class,     // mapper class
        Text.class,         // mapper output key
        FloatArrayWritable.class,  // mapper output value
        job);
    job.setReducerClass(CustomReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, new Path(args[0]));
    if (job.waitForCompletion(true)) {
      hBaseService.close();
      System.exit(0);
    } else {
      System.exit(1);
    }
  }
}
