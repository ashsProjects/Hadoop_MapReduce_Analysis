import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class CountAverageTuple implements Writable {
    private int count = 0;
    private double average;
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeDouble(average);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        average = in.readDouble();
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }
    
    @Override
    public String toString() {
        return Double.toString(average);
    }
}