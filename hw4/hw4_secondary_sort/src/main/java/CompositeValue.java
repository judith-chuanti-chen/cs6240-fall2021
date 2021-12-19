import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValue implements Writable {
    private int month;
    private float arrDelayedMinutes;

    public CompositeValue() {
    }

    public CompositeValue(int month, float arrDelayedMinutes) {
        this.month = month;
        this.arrDelayedMinutes = arrDelayedMinutes;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(month);
        dataOutput.writeFloat(arrDelayedMinutes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.month = dataInput.readInt();
        this.arrDelayedMinutes = dataInput.readFloat();
    }

    public int getMonth() {
        return month;
    }

    public float getArrDelayedMinutes() {
        return arrDelayedMinutes;
    }

    @Override
    public String toString() {
        return "CompositeValue{" +
                "month=" + month +
                ", arrDelayedMinutes=" + arrDelayedMinutes +
                '}';
    }
}
