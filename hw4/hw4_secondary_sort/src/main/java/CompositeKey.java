import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private String airlineId;
    private int month;

    public CompositeKey() {
    }

    public CompositeKey(String airlineId, int month) {
        this.airlineId = airlineId;
        this.month = month;
    }

    @Override
    public int compareTo(CompositeKey key) {
        if (this.airlineId.equals(key.airlineId)){
            return Integer.compare(this.month, key.getMonth());
        }
        return this.airlineId.compareTo(key.getAirlineId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(airlineId);
        dataOutput.writeInt(month);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.airlineId = dataInput.readUTF();
        this.month = dataInput.readInt();
    }

    public String getAirlineId() {
        return airlineId;
    }

    public int getMonth() {
        return month;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {return true;}
        if (obj == null || getClass() != obj.getClass()) {return false;}
        CompositeKey other = (CompositeKey) obj;
        return getAirlineId() == other.getAirlineId() && getMonth() == other.getMonth();
    }

    @Override
    public int hashCode() {
        return 31 * getAirlineId().hashCode() + getMonth();
    }
}
