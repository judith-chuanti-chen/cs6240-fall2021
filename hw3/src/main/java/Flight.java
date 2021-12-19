public class Flight {
    private int year;
    private int month;
    private String flightDate;
    private String origin;
    private String dest;
    private int depTime;
    private int arrTime;
    private float arrDelayMinutes;
    private int cancelled;
    private int diverted;
    private int leg;

    // Constructor used in mapper
    public Flight(int year, int month, String flightDate, String origin, String dest, int depTime, int arrTime,
                  float arrDelayMinutes, int cancelled, int diverted) {
        this.year = year;
        this.month = month;
        this.flightDate = flightDate;
        this.origin = origin;
        this.dest = dest;
        this.depTime = depTime;
        this.arrTime = arrTime;
        this.arrDelayMinutes = arrDelayMinutes;
        this.cancelled = cancelled;
        this.diverted = diverted;
        this.leg = 0;
    }

    public Flight(String[] args) {
        this.depTime = Integer.parseInt(args[0]);
        this.arrTime = Integer.parseInt(args[1]);
        this.arrDelayMinutes = Float.parseFloat(args[2]);
        this.leg = Integer.parseInt(args[3]);
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public String getFlightDate() {
        return flightDate;
    }

    public String getOrigin() {
        return origin;
    }

    public String getDest() {
        return dest;
    }

    public int getDepTime() {
        return depTime;
    }

    public int getArrTime() {
        return arrTime;
    }

    public float getArrDelayMinutes() {
        return arrDelayMinutes;
    }

    public int getCancelled() {
        return cancelled;
    }

    public int getDiverted() {
        return diverted;
    }

    public int getLeg() {
        return leg;
    }

    public void setLeg(int leg) {
        this.leg = leg;
    }

    // emit (key, value) where key = [date, airport] and value = flight.toString() (i.e. depTime, arrTime,
    // arrDelayMinutes, leg)
    @Override
    public String toString() {
        return  depTime + "," + arrTime + "," + arrDelayMinutes + "," + leg;
    }
}
