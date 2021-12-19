import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Bike {
  private String startTime;
  private int startStationId;
  private int endStationId;
  private int duration;

  public Bike(String startTime, int startStationId, int endStationId, int duration) {
    this.startTime = startTime;
    this.startStationId = startStationId;
    this.endStationId = endStationId;
    this.duration = duration;
  }

  public String getStartTime() {
    return startTime;
  }

  public int getStartStationId() {
    return startStationId;
  }

  public int getEndStationId() {
    return endStationId;
  }

  public int getDuration() {
    return duration;
  }

  /**
   * returns a byte[] row key for the HBase table. The key contains
   the
   *
   * @return
   */
  public byte[] createRowKey() {
    String key = this.startTime;
    // The byte[] table row key
    byte[] rKey = new byte[2 * Bytes.SIZEOF_LONG];
    Bytes.putBytes(rKey, 0, Bytes.toBytes(key), 0, Bytes.SIZEOF_LONG);

    // Add timestamp to rowKey
    long timeStamp = System.nanoTime();
    Bytes.putLong(rKey, Bytes.SIZEOF_LONG, timeStamp);

    return rKey;
  }

  public Put createRow(Put put) {
    put.addColumn(
        Bytes.toBytes(BikeDataConstant.COL_FAMILY),
        Bytes.toBytes(BikeDataConstant.START_TIME),
        Bytes.toBytes(startTime));
    put.addColumn(
        Bytes.toBytes(BikeDataConstant.COL_FAMILY),
        Bytes.toBytes(BikeDataConstant.START_STATION_ID),
        Bytes.toBytes(String.valueOf(startStationId)));
    put.addColumn(
        Bytes.toBytes(BikeDataConstant.COL_FAMILY),
        Bytes.toBytes(BikeDataConstant.END_STATION_ID),
        Bytes.toBytes(String.valueOf(endStationId)));
    put.addColumn(
        Bytes.toBytes(BikeDataConstant.COL_FAMILY),
        Bytes.toBytes(BikeDataConstant.DURATION),
        Bytes.toBytes(String.valueOf(duration)));
    return put;
  }
}
