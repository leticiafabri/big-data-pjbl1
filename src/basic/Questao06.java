package basic;

import advanced.customwritable.MinMaxWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class Questao06 {

    // MAP
    public static class Map extends Mapper<LongWritable, Text, Text, MinMaxWritable> {

        private Text brasil2016 = new Text("Brazil-2016");

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Remove cabeçalho
            if (PreProcessamento.isHeader(line)) {
                return;
            }

            String[] fields = line.split(";");

            // Trata dados faltantes
            if (!PreProcessamento.isValid(fields)) {
                return;
            }

            String country = fields[0];
            String year = fields[1];

            // Apenas Brasil em 2016
            if (!country.equalsIgnoreCase("Brazil")) {
                return;
            }

            if (!year.equals("2016")) {
                return;
            }

            double price;

            try {
                price = Double.parseDouble(fields[5]);
            } catch (Exception e) {
                return;
            }

            con.write(brasil2016, new MinMaxWritable(price, price));
        }
    }

    // COMBINER
    public static class Combiner extends Reducer<Text, MinMaxWritable, Text, MinMaxWritable> {

        public void reduce(Text key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (MinMaxWritable val : values) {

                if (val.getMin() < min) {
                    min = val.getMin();
                }

                if (val.getMax() > max) {
                    max = val.getMax();
                }
            }

            con.write(key, new MinMaxWritable(min, max));
        }
    }

    // REDUCE
    public static class Reduce extends Reducer<Text, MinMaxWritable, Text, Text> {

        public void reduce(Text key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (MinMaxWritable val : values) {

                if (val.getMin() < min) {
                    min = val.getMin();
                }

                if (val.getMax() > max) {
                    max = val.getMax();
                }
            }

            con.write(key, new Text(
                    "Menor preço: " + min + " | Maior preço: " + max
            ));
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job j = Job.getInstance(conf, "Questao06");

        j.setJarByClass(Questao06.class);

        j.setMapperClass(Map.class);

        // COMBINER obrigatório
        j.setCombinerClass(Combiner.class);

        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MinMaxWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j,
                new Path("in/operacoes_comerciais_inteira.csv"));

        FileOutputFormat.setOutputPath(j,
                new Path("output/resultado_questao06"));

        boolean sucesso = j.waitForCompletion(true);

        if (sucesso) {

            Files.copy(
                    Paths.get("output/resultado_questao06/part-r-00000"),
                    Paths.get("output/resultado-06.txt"),
                    StandardCopyOption.REPLACE_EXISTING
            );

            System.out.println("Resultado salvo em output/resultado-06.txt");
        }

        System.exit(sucesso ? 0 : 1);
    }
}