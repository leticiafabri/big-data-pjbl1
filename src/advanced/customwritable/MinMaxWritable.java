package advanced.customwritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxWritable implements Writable {

    private DoubleWritable min;
    private DoubleWritable max;

    public MinMaxWritable() {
        this.min = new DoubleWritable();
        this.max = new DoubleWritable();
    }

    public MinMaxWritable(double min, double max) {
        this.min = new DoubleWritable(min);
        this.max = new DoubleWritable(max);
    }

    public double getMin() {
        return min.get();
    }

    public double getMax() {
        return max.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        min.write(out);
        max.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        min.readFields(in);
        max.readFields(in);
    }
}