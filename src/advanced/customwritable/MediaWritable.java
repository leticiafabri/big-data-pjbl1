package advanced.customwritable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MediaWritable implements Writable {

    private DoubleWritable soma;
    private IntWritable quantidade;

    public MediaWritable() {
        this.soma = new DoubleWritable();
        this.quantidade = new IntWritable();
    }

    public MediaWritable(double soma, int quantidade) {
        this.soma = new DoubleWritable(soma);
        this.quantidade = new IntWritable(quantidade);
    }

    public double getSoma() {
        return soma.get();
    }

    public int getQuantidade() {
        return quantidade.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        soma.write(out);
        quantidade.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        soma.readFields(in);
        quantidade.readFields(in);
    }
}