package basic;

import advanced.customwritable.MediaWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Questao05 {

    // MAP
    public static class Map extends Mapper<LongWritable, Text, Text, MediaWritable> {

        private Text yearText = new Text();

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

            // Apenas Brasil
            if (!country.equalsIgnoreCase("Brazil")) {
                return;
            }

            String year = fields[1];

            // Price
            double price;

            try {
                price = Double.parseDouble(fields[5]);
            } catch (Exception e) {
                return;
            }

            yearText.set(year);

            con.write(yearText, new MediaWritable(price, 1));
        }
    }

    // REDUCE
    public static class Reduce extends Reducer<Text, MediaWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<MediaWritable> values, Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            int quantidade = 0;

            for (MediaWritable val : values) {
                soma += val.getSoma();
                quantidade += val.getQuantidade();
            }

            double media = soma / quantidade;

            con.write(key, new DoubleWritable(media));
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job j = Job.getInstance(conf, "Questao05");

        j.setJarByClass(Questao05.class);

        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MediaWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j,
                new Path("in/operacoes_comerciais_inteira.csv"));

        FileOutputFormat.setOutputPath(j,
                new Path("output/resultado_questao05"));

        boolean sucesso = j.waitForCompletion(true);

        if (sucesso) {

            Files.copy(
                    Paths.get("output/resultado_questao05/part-r-00000"),
                    Paths.get("output/resultado-05.txt"),
                    StandardCopyOption.REPLACE_EXISTING
            );

            System.out.println("Resultado salvo em output/resultado-05.txt");
        }

        System.exit(sucesso ? 0 : 1);
    }
}