import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Flight {
    private String year;
    private String month;
    private String airlineId;
    private String arrDelayMinutes;
    private String cancelled;
    private String diverted;

    public Flight(String year, String month, String airlineId, String arrDelayMinutes, String cancelled, String diverted) {
        this.year = year;
        this.month = month;
        this.airlineId = airlineId;
        this.arrDelayMinutes = arrDelayMinutes;
        this.cancelled = cancelled;
        this.diverted = diverted;
    }

    public byte[] createRowKey() {
        long timestamp = System.nanoTime();
        byte[] a = Bytes.add(Bytes.toBytes(year), Bytes.toBytes(cancelled), Bytes.toBytes(diverted));
        byte[] b = Bytes.add(Bytes.toBytes(month), Bytes.toBytes(airlineId), Bytes.toBytes(timestamp));
        System.out.println(Bytes.add(a, b).toString());
        return Bytes.add(a, b);
    }

    public Put createRow(Put put) {
        put.addColumn(
                Bytes.toBytes(FlightDataConstants.COL_FAMILY),
                Bytes.toBytes(FlightDataConstants.AIRLINE_ID),
                Bytes.toBytes(airlineId));
        put.addColumn(
                Bytes.toBytes(FlightDataConstants.COL_FAMILY),
                Bytes.toBytes(FlightDataConstants.MONTH),
                Bytes.toBytes(month));
        put.addColumn(
                Bytes.toBytes(FlightDataConstants.COL_FAMILY),
                Bytes.toBytes(FlightDataConstants.ARR_DELAY_MINUTES),
                Bytes.toBytes(arrDelayMinutes));
        return put;
    }
    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getAirlineId() {
        return airlineId;
    }

    public String getArrDelayMinutes() {
        return arrDelayMinutes;
    }

    public String getCancelled() {
        return cancelled;
    }

    public String getDiverted() {
        return diverted;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public void setAirlineId(String airlineId) {
        this.airlineId = airlineId;
    }

    public void setArrDelayMinutes(String arrDelayMinutes) {
        this.arrDelayMinutes = arrDelayMinutes;
    }

    public void setCancelled(String cancelled) {
        this.cancelled = cancelled;
    }

    public void setDiverted(String diverted) {
        this.diverted = diverted;
    }
}
