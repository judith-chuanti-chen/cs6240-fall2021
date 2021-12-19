import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.ArrayList;
import java.util.List;

public class FlightData {
    private String year;
    private String quarter;
    private String month;
    private String dayOfMonth;
    private String dayOfWeek;
    private String flightDate;
    private String uniqueCarrier;
    private String airlineId;
    private String carrier;
    private String tailNum;
    private String flightNum;
    private String origin;
    private String originCityName;
    private String originState;
    private String originStateFips;
    private String originStateName;
    private String originWac;
    private String dest;
    private String destCityName;
    private String destState;
    private String destStateFips;
    private String destStateName;
    private String destWac;
    private String crsDepTime;
    private String depTime;
    private String depDelay;
    private String depDelayMinutes;
    private String depDel15;
    private String departureDelayGroups;
    private String depTimeBlk;
    private String taxiOut;
    private String wheelsOff;
    private String wheelsOn;
    private String taxiIn;
    private String crsArrTime;
    private String arrTime;
    private String arrDelay;
    private String arrDelayMinutes;
    private String arrDel15;
    private String arrivalDelayGroups;
    private String arrTimeBlk;
    private String cancelled;
    private String cancellationCode;
    private String diverted;
    private String crsElapsedTime;
    private String actualElapsedTime;
    private String airTime;
    private String flights;
    private String distance;
    private String distanceGroup;
    private String carrierDelay;
    private String weatherDelay;
    private String nasDelay;
    private String securityDelay;
    private String lateAircraftDelay;

    private FlightData(){}

    public static FlightData parseRow(String[] line){
        FlightData flightData = new FlightData();
        flightData.setYear(line[0]);
        flightData.setQuarter(line[1]);
        flightData.setMonth(line[2]);
        flightData.setDayOfMonth(line[3]);
        flightData.setDayOfWeek(line[4]);
        flightData.setFlightDate(line[5]);
        flightData.setUniqueCarrier(line[6]);
        flightData.setAirlineId(line[7]);
        flightData.setCarrier(line[8]);
        flightData.setTailNum(line[9]);
        flightData.setFlightNum(line[10]);
        flightData.setOrigin(line[11]);
        flightData.setOriginCityName(line[12]);
        flightData.setOriginState(line[13]);
        flightData.setOriginStateFips(line[14]);
        flightData.setOriginStateName(line[15]);
        flightData.setOriginWac(line[16]);
        flightData.setDest(line[17]);
        flightData.setDestCityName(line[18]);
        flightData.setDestState(line[19]);
        flightData.setDestStateFips(line[20]);
        flightData.setDestStateName(line[21]);
        flightData.setDestWac(line[22]);
        flightData.setCrsDepTime(line[23]);
        flightData.setDepTime(line[24]);
        flightData.setDepDelay(line[25]);
        flightData.setDepDelayMinutes(line[26]);
        flightData.setDepDel15(line[27]);
        flightData.setDepartureDelayGroups(line[28]);
        flightData.setDepTimeBlk(line[29]);
        flightData.setTaxiOut(line[30]);
        flightData.setWheelsOff(line[31]);
        flightData.setWheelsOn(line[32]);
        flightData.setTaxiIn(line[33]);
        flightData.setCrsArrTime(line[34]);
        flightData.setArrTime(line[35]);
        flightData.setArrDelay(line[36]);
        flightData.setArrDelayMinutes(line[37]);
        flightData.setArrDel15(line[38]);
        flightData.setArrivalDelayGroups(line[39]);
        flightData.setArrTimeBlk(line[40]);
        flightData.setCancelled(line[41]);
        flightData.setCancellationCode(line[42]);
        flightData.setDiverted(line[43]);
        flightData.setCrsElapsedTime(line[44]);
        flightData.setActualElapsedTime(line[45]);
        flightData.setAirTime(line[46]);
        flightData.setFlights(line[47]);
        flightData.setDistance(line[48]);
        flightData.setDistanceGroup(line[49]);
        flightData.setCarrierDelay(line[50]);
        flightData.setWeatherDelay(line[51]);
        flightData.setNasDelay(line[52]);
        flightData.setSecurityDelay(line[53]);
        flightData.setLateAircraftDelay(line[54]);
        return flightData;
    }

    public byte[] createRowKey() {
        long timestamp = System.nanoTime();
        byte[] a = Bytes.add(Bytes.toBytes(year), Bytes.toBytes(cancelled), Bytes.toBytes(diverted));
        byte[] b = Bytes.add(Bytes.toBytes(month), Bytes.toBytes(airlineId), Bytes.toBytes(timestamp));
        return Bytes.add(a, b);
    }


    public Put createRow(Put put) {
        List<byte[]> listOfBytes = new ArrayList<byte[]>(){
            {
                add(Bytes.toBytes(year));
                add(Bytes.toBytes(quarter));
                add(Bytes.toBytes(month));
                add(Bytes.toBytes(dayOfMonth));
                add(Bytes.toBytes(dayOfWeek));
                add(Bytes.toBytes(flightDate));
                add(Bytes.toBytes(uniqueCarrier));
                add(Bytes.toBytes(airlineId));
                add(Bytes.toBytes(carrier));
                add(Bytes.toBytes(tailNum));
                add(Bytes.toBytes(flightNum));
                add(Bytes.toBytes(origin));
                add(Bytes.toBytes(originCityName));
                add(Bytes.toBytes(originState));
                add(Bytes.toBytes(originStateFips));
                add(Bytes.toBytes(originStateName));
                add(Bytes.toBytes(originWac));
                add(Bytes.toBytes(dest));
                add(Bytes.toBytes(destCityName));
                add(Bytes.toBytes(destState));
                add(Bytes.toBytes(destStateFips));
                add(Bytes.toBytes(destStateName));
                add(Bytes.toBytes(destWac));
                add(Bytes.toBytes(crsDepTime));
                add(Bytes.toBytes(depTime));
                add(Bytes.toBytes(depDelay));
                add(Bytes.toBytes(depDelayMinutes));
                add(Bytes.toBytes(depDel15));
                add(Bytes.toBytes(departureDelayGroups));
                add(Bytes.toBytes(depTimeBlk));
                add(Bytes.toBytes(taxiOut));
                add(Bytes.toBytes(wheelsOff));
                add(Bytes.toBytes(wheelsOn));
                add(Bytes.toBytes(taxiIn));
                add(Bytes.toBytes(crsArrTime));
                add(Bytes.toBytes(arrTime));
                add(Bytes.toBytes(arrDelay));
                add(Bytes.toBytes(arrDelayMinutes));
                add(Bytes.toBytes(arrDel15));
                add(Bytes.toBytes(arrivalDelayGroups));
                add(Bytes.toBytes(arrTimeBlk));
                add(Bytes.toBytes(cancelled));
                add(Bytes.toBytes(cancellationCode));
                add(Bytes.toBytes(diverted));
                add(Bytes.toBytes(crsElapsedTime));
                add(Bytes.toBytes(actualElapsedTime));
                add(Bytes.toBytes(airTime));
                add(Bytes.toBytes(flights));
                add(Bytes.toBytes(distance));
                add(Bytes.toBytes(distanceGroup));
                add(Bytes.toBytes(carrierDelay));
                add(Bytes.toBytes(weatherDelay));
                add(Bytes.toBytes(nasDelay));
                add(Bytes.toBytes(securityDelay));
                add(Bytes.toBytes(lateAircraftDelay));
            }
        };
        for(int i = 0; i < listOfBytes.size(); i++){
            String qualifier = FlightDataConstants.COLUMN_QUALIFIERS.get(i);
            put.addColumn(Bytes.toBytes(FlightDataConstants.COL_FAMILY), Bytes.toBytes(qualifier), listOfBytes.get(i));
        }
        return put;
    }

    public String getYear() {
        return year;
    }

    public String getQuarter() {
        return quarter;
    }

    public String getMonth() {
        return month;
    }

    public String getDayOfMonth() {
        return dayOfMonth;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public String getFlightDate() {
        return flightDate;
    }

    public String getUniqueCarrier() {
        return uniqueCarrier;
    }

    public String getAirlineId() {
        return airlineId;
    }

    public String getCarrier() {
        return carrier;
    }

    public String getTailNum() {
        return tailNum;
    }

    public String getFlightNum() {
        return flightNum;
    }

    public String getOrigin() {
        return origin;
    }

    public String getOriginCityName() {
        return originCityName;
    }

    public String getOriginState() {
        return originState;
    }

    public String getOriginStateFips() {
        return originStateFips;
    }

    public String getOriginStateName() {
        return originStateName;
    }

    public String getOriginWac() {
        return originWac;
    }

    public String getDest() {
        return dest;
    }

    public String getDestCityName() {
        return destCityName;
    }

    public String getDestState() {
        return destState;
    }

    public String getDestStateFips() {
        return destStateFips;
    }

    public String getDestStateName() {
        return destStateName;
    }

    public String getDestWac() {
        return destWac;
    }

    public String getCrsDepTime() {
        return crsDepTime;
    }

    public String getDepTime() {
        return depTime;
    }

    public String getDepDelay() {
        return depDelay;
    }

    public String getDepDelayMinutes() {
        return depDelayMinutes;
    }

    public String getDepDel15() {
        return depDel15;
    }

    public String getDepartureDelayGroups() {
        return departureDelayGroups;
    }

    public String getDepTimeBlk() {
        return depTimeBlk;
    }

    public String getTaxiOut() {
        return taxiOut;
    }

    public String getWheelsOff() {
        return wheelsOff;
    }

    public String getWheelsOn() {
        return wheelsOn;
    }

    public String getTaxiIn() {
        return taxiIn;
    }

    public String getCrsArrTime() {
        return crsArrTime;
    }

    public String getArrTime() {
        return arrTime;
    }

    public String getArrDelay() {
        return arrDelay;
    }

    public String getArrDelayMinutes() {
        return arrDelayMinutes;
    }

    public String getArrDel15() {
        return arrDel15;
    }

    public String getArrivalDelayGroups() {
        return arrivalDelayGroups;
    }

    public String getArrTimeBlk() {
        return arrTimeBlk;
    }

    public String getCancelled() {
        return cancelled;
    }

    public String getCancellationCode() {
        return cancellationCode;
    }

    public String getDiverted() {
        return diverted;
    }

    public String getCrsElapsedTime() {
        return crsElapsedTime;
    }

    public String getActualElapsedTime() {
        return actualElapsedTime;
    }

    public String getAirTime() {
        return airTime;
    }

    public String getFlights() {
        return flights;
    }

    public String getDistance() {
        return distance;
    }

    public String getDistanceGroup() {
        return distanceGroup;
    }

    public String getCarrierDelay() {
        return carrierDelay;
    }

    public String getWeatherDelay() {
        return weatherDelay;
    }

    public String getNasDelay() {
        return nasDelay;
    }

    public String getSecurityDelay() {
        return securityDelay;
    }

    public String getLateAircraftDelay() {
        return lateAircraftDelay;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public void setQuarter(String quarter) {
        this.quarter = quarter;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public void setDayOfMonth(String dayOfMonth) {
        this.dayOfMonth = dayOfMonth;
    }

    public void setDayOfWeek(String dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public void setFlightDate(String flightDate) {
        this.flightDate = flightDate;
    }

    public void setUniqueCarrier(String uniqueCarrier) {
        this.uniqueCarrier = uniqueCarrier;
    }

    public void setAirlineId(String airlineId) {
        this.airlineId = airlineId;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public void setTailNum(String tailNum) {
        this.tailNum = tailNum;
    }

    public void setFlightNum(String flightNum) {
        this.flightNum = flightNum;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public void setOriginCityName(String originCityName) {
        this.originCityName = originCityName;
    }

    public void setOriginState(String originState) {
        this.originState = originState;
    }

    public void setOriginStateFips(String originStateFips) {
        this.originStateFips = originStateFips;
    }

    public void setOriginStateName(String originStateName) {
        this.originStateName = originStateName;
    }

    public void setOriginWac(String originWac) {
        this.originWac = originWac;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public void setDestCityName(String destCityName) {
        this.destCityName = destCityName;
    }

    public void setDestState(String destState) {
        this.destState = destState;
    }

    public void setDestStateFips(String destStateFips) {
        this.destStateFips = destStateFips;
    }

    public void setDestStateName(String destStateName) {
        this.destStateName = destStateName;
    }

    public void setDestWac(String destWac) {
        this.destWac = destWac;
    }

    public void setCrsDepTime(String crsDepTime) {
        this.crsDepTime = crsDepTime;
    }

    public void setDepTime(String depTime) {
        this.depTime = depTime;
    }

    public void setDepDelay(String depDelay) {
        this.depDelay = depDelay;
    }

    public void setDepDelayMinutes(String depDelayMinutes) {
        this.depDelayMinutes = depDelayMinutes;
    }

    public void setDepDel15(String depDel15) {
        this.depDel15 = depDel15;
    }

    public void setDepartureDelayGroups(String departureDelayGroups) {
        this.departureDelayGroups = departureDelayGroups;
    }

    public void setDepTimeBlk(String depTimeBlk) {
        this.depTimeBlk = depTimeBlk;
    }

    public void setTaxiOut(String taxiOut) {
        this.taxiOut = taxiOut;
    }

    public void setWheelsOff(String wheelsOff) {
        this.wheelsOff = wheelsOff;
    }

    public void setWheelsOn(String wheelsOn) {
        this.wheelsOn = wheelsOn;
    }

    public void setTaxiIn(String taxiIn) {
        this.taxiIn = taxiIn;
    }

    public void setCrsArrTime(String crsArrTime) {
        this.crsArrTime = crsArrTime;
    }

    public void setArrTime(String arrTime) {
        this.arrTime = arrTime;
    }

    public void setArrDelay(String arrDelay) {
        this.arrDelay = arrDelay;
    }

    public void setArrDelayMinutes(String arrDelayMinutes) {
        this.arrDelayMinutes = arrDelayMinutes;
    }

    public void setArrDel15(String arrDel15) {
        this.arrDel15 = arrDel15;
    }

    public void setArrivalDelayGroups(String arrivalDelayGroups) {
        this.arrivalDelayGroups = arrivalDelayGroups;
    }

    public void setArrTimeBlk(String arrTimeBlk) {
        this.arrTimeBlk = arrTimeBlk;
    }

    public void setCancelled(String cancelled) {
        this.cancelled = cancelled;
    }

    public void setCancellationCode(String cancellationCode) {
        this.cancellationCode = cancellationCode;
    }

    public void setDiverted(String diverted) {
        this.diverted = diverted;
    }

    public void setCrsElapsedTime(String crsElapsedTime) {
        this.crsElapsedTime = crsElapsedTime;
    }

    public void setActualElapsedTime(String actualElapsedTime) {
        this.actualElapsedTime = actualElapsedTime;
    }

    public void setAirTime(String airTime) {
        this.airTime = airTime;
    }

    public void setFlights(String flights) {
        this.flights = flights;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public void setDistanceGroup(String distanceGroup) {
        this.distanceGroup = distanceGroup;
    }

    public void setCarrierDelay(String carrierDelay) {
        this.carrierDelay = carrierDelay;
    }

    public void setWeatherDelay(String weatherDelay) {
        this.weatherDelay = weatherDelay;
    }

    public void setNasDelay(String nasDelay) {
        this.nasDelay = nasDelay;
    }

    public void setSecurityDelay(String securityDelay) {
        this.securityDelay = securityDelay;
    }

    public void setLateAircraftDelay(String lateAircraftDelay) {
        this.lateAircraftDelay = lateAircraftDelay;
    }
}
