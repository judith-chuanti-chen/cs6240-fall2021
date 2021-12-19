public class Flight {
    private int year;
    private int month;
    private String airlineId;
    private float arrDelayMinutes;
    private int cancelled;
    private int diverted;

    // Constructor used in mapper

    public Flight(int year, int month, String airlineId, float arrDelayMinutes, int cancelled, int diverted) {
        this.year = year;
        this.month = month;
        this.airlineId = airlineId;
        this.arrDelayMinutes = arrDelayMinutes;
        this.cancelled = cancelled;
        this.diverted = diverted;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public String getAirlineId() { return airlineId; }

    public float getArrDelayMinutes() {
        return arrDelayMinutes;
    }

    public int getCancelled() {
        return cancelled;
    }

    public int getDiverted() {
        return diverted;
    }
}
