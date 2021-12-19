import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements Comparable<Pair>, Writable {
    private int startStationId;
    private int endStationId;
    private int count;

    public Pair() {
    }

    public Pair(int startStationId, int endStationId, int count) {
        this.startStationId = startStationId;
        this.endStationId = endStationId;
        this.count = count;
    }

    @Override
    public int compareTo(Pair o) {
        if (this.count == o.count && this.startStationId == o.startStationId) {
            return this.endStationId - o.endStationId;
        } else if (this.count == o.count) {
            return this.startStationId - o.startStationId;
        }
        return this.count - o.count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(startStationId);
        dataOutput.writeInt(endStationId);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.startStationId = dataInput.readInt();
        this.endStationId = dataInput.readInt();
        this.count = dataInput.readInt();
    }

    public int getStartStationId() {
        return startStationId;
    }

    public int getEndStationId() {
        return endStationId;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "startStationId=" + startStationId +
                ", endStationId=" + endStationId +
                ", count=" + count +
                '}';
    }
}
