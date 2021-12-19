import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseCompute {
    public static class Mapper extends TableMapper<Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result value, Context context) throws IOException, InterruptedException {

            String outKey = new String(value.getValue(Bytes.toBytes(FlightDataConstants.COL_FAMILY),
                            Bytes.toBytes(FlightDataConstants.AIRLINE_ID)));
            String month = new String(value.getValue(Bytes.toBytes(FlightDataConstants.COL_FAMILY),
                    Bytes.toBytes(FlightDataConstants.MONTH)));
            String delayMin = new String(value.getValue(Bytes.toBytes(FlightDataConstants.COL_FAMILY),
                    Bytes.toBytes(FlightDataConstants.ARR_DELAY_MINUTES)));
            String outValue = month + "," + delayMin;
            context.write(new Text(outKey), new Text(outValue));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class CustomReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float[] delayPerMonth = new float[12];
            int[] countPerMonth = new int[12];
            for (Text value : values) {
                String[] strArr = value.toString().split(",");
                int month = Integer.parseInt(strArr[0]);
                float delayMin = Float.parseFloat(strArr[1]);
                delayPerMonth[month-1] += delayMin;
                countPerMonth[month-1] += 1;
            }
            List<String> resultList = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                int avgMinPerMonth = 0;
                if(countPerMonth[i] > 0){
                    avgMinPerMonth = (int) Math.ceil(delayPerMonth[i] / countPerMonth[i]);
                }
                String result = "(" + (i+1) + ", " + avgMinPerMonth + ")";
                resultList.add(result);
            }
            context.write(key, new Text(String.join(", ", resultList)));
        }
    }
    public static Scan getFilteredScan(){
        // Year=2018, cancelled = 0, diverted = 0
        String startRowKeyPrefix = "20080.000.00";
        // Year=2018, cancelled = 0, diverted = 1 (row keys are sorted in HBase, hence no need to consider "201810" or "201811")
        String stopRowKeyPrefix = "20080.001.00";
        Scan scan = new Scan()
                .withStartRow(Bytes.toBytes(startRowKeyPrefix))
                .withStopRow(Bytes.toBytes(stopRowKeyPrefix));
        // Higher caching values enable faster scanners but use more memory
        scan.setCaching(10000);
        // don't set to true for MR jobs
        scan.setCacheBlocks(false);
        return scan;
    }

    public static void main(String[] args) throws Exception {
        HBaseService hBaseService = HBaseService.createHBaseService();
        Job job = Job.getInstance(hBaseService.getConf(),"HBaseCompute");
        job.setJarByClass(HBaseCompute.class);

        Scan scan = getFilteredScan();
        TableMapReduceUtil.initTableMapperJob(
                FlightDataConstants.TABLE_NAME,        // input table
                scan,               // Scan instance to control CF and attribute selection
                Mapper.class,     // mapper class
                Text.class,         // mapper output key
                Text.class,  // mapper output value
                job);
        job.setReducerClass(CustomReducer.class);
        job.setNumReduceTasks(5);
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
