
import com.opencsv.CSVReader;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;
import java.io.StringReader;

public class HBasePopulate {
    private enum MapCounter{
        MAP_TASK
    }

    public static class CustomMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
        private CSVReader csvReader;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if ( ((LongWritable) key).get() == 0 && value.toString().contains("Year")) return;

            csvReader = new CSVReader(new StringReader(value.toString()));
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                String year = line[0], month = line[2], airlineId = line[7], arrDelayMin = line[37], cancelled = line[41],
                        diverted = line[43];
                Flight f = new Flight(year, month, airlineId, arrDelayMin, cancelled, diverted);
                byte[] rowKey = f.createRowKey();
                Put put = new Put(rowKey);
                put = f.createRow(put);
                context.write(new ImmutableBytesWritable(rowKey), put);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            csvReader.close();
            context.getCounter(MapCounter.MAP_TASK).increment(1);
        }
    }

    public static void main(String[] args) throws Exception {
        HBaseService hBaseService = HBaseService.createHBaseService();
        Table table = hBaseService.createTable(FlightDataConstants.TABLE_NAME, FlightDataConstants.COL_FAMILY);
        String[] otherArgs = new GenericOptionsParser(hBaseService.getConf(), args)
                .getRemainingArgs();
        Job job = Job.getInstance(hBaseService.getConf(), "HBase table populate");
        job.setJarByClass(HBasePopulate.class);
        job.setMapperClass(CustomMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableReducerJob(FlightDataConstants.TABLE_NAME, null, job);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, FlightDataConstants.FLIGHTS);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        System.out.println("Set up job OK.");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        table.close();
        hBaseService.close();
    }
}
