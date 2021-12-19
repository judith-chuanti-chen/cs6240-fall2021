import com.opencsv.CSVReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HBasePopulate {
  private enum MapCounter{
    MAP_TASK
  }

  public static class HPopulateMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
    private CSVReader csvReader;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      if ( ((LongWritable) key).get() == 0 && value.toString().contains("rental_id")) return;
      csvReader = new CSVReader(new StringReader(value.toString()));
      String[] line;

      while ((line = csvReader.readNext()) != null) {
        String startRentalDateTime = line[6];
        int startStationId = line[7].length() > 0 ? Integer.parseInt(line[7]) : -1,
            endStationId = line[4].length() > 0 ? Double.valueOf(line[4]).intValue() : -1;
        int duration = line[1].length() > 0 ? Double.valueOf(line[1]).intValue() : -1;
        if (startRentalDateTime.length() < 10 || startStationId < 0 || endStationId < 0 || duration < 0) {
          continue;
        }
        String startTime;

        try {
          String[] dateSplit = startRentalDateTime.split(" ");
          startTime = dateSplit[dateSplit.length - 1].trim();
          DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");
          Date d = dateFormat.parse(startTime);
        } catch (DateTimeParseException | ParseException e) {
          continue;
        }
        Bike bike = new Bike(startTime, startStationId, endStationId, duration);
        byte[] rowKey = bike.createRowKey();
        Put put = new Put(rowKey);
        put = bike.createRow(put);
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
    Table table = hBaseService.createTable(BikeDataConstant.TABLE_NAME, BikeDataConstant.COL_FAMILY);
    String[] otherArgs = new GenericOptionsParser(hBaseService.getConf(), args)
        .getRemainingArgs();
    Job job = Job.getInstance(hBaseService.getConf(), "HBase table populate");
    job.setJarByClass(HBasePopulate.class);
    job.setMapperClass(HPopulateMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setNumReduceTasks(0);
    TableMapReduceUtil.initTableReducerJob(BikeDataConstant.TABLE_NAME, null, job);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, BikeDataConstant.BIKES);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

    System.out.println("Set up job OK.");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    table.close();
    hBaseService.close();
  }
}
