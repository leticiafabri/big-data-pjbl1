package basic;

import advanced.customwritable.MinMaxWritable;
import advanced.customwritable.YearCountryKey;
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

public class Questao08 {

    // MAP
    public static class Map extends Mapper<LongWritable, Text, YearCountryKey, MinMaxWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            //remove cabeçalho
            if (PreProcessamento.isHeader(line)) {
                return;
            }

            String[] fields = line.split(";");

            //trata dados faltantes
            if (!PreProcessamento.isValid(fields)) {
                return;
            }

            String country = fields[0];
            String year = fields[1];
            String amountString = fields[8];

            //ignora campos vazios
            if (country.trim().isEmpty()
                    || year.trim().isEmpty()
                    || amountString.trim().isEmpty()) {
                return;
            }

            amountString = amountString.replace(",", ".");

            double amount;

            try {
                amount = Double.parseDouble(amountString);
            } catch (NumberFormatException e) {
                return;
            }

            YearCountryKey outKey =
                    new YearCountryKey(year, country);

            con.write(outKey,
                    new MinMaxWritable(amount, amount));
        }
    }

    // COMBINER
    public static class Combiner extends Reducer<YearCountryKey, MinMaxWritable, YearCountryKey, MinMaxWritable> {

        public void reduce(YearCountryKey key,
                           Iterable<MinMaxWritable> values,
                           Context con)
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

            con.write(key,
                    new MinMaxWritable(min, max));
        }
    }

    // REDUCE
    public static class Reduce extends Reducer<YearCountryKey, MinMaxWritable, YearCountryKey, Text> {

        private Text result = new Text();

        public void reduce(YearCountryKey key,
                           Iterable<MinMaxWritable> values,
                           Context con)
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

            result.set("Menor: " + min + " | Maior: " + max);

            con.write(key, result);
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job j = Job.getInstance(conf, "Questao08");

        j.setJarByClass(Questao08.class);

        j.setMapperClass(Map.class);

        //combiner obrigatório
        j.setCombinerClass(Combiner.class);

        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(YearCountryKey.class);
        j.setMapOutputValueClass(MinMaxWritable.class);

        j.setOutputKeyClass(YearCountryKey.class);
        j.setOutputValueClass(Text.class);

        //arquivo de entrada
        FileInputFormat.addInputPath(j,
                new Path("in/operacoes_comerciais_inteira.csv"));

        //pasta temporária de saída
        FileOutputFormat.setOutputPath(j,
                new Path("output/resultado_questao08"));

        boolean sucesso = j.waitForCompletion(true);

        //copia resultado final para TXT
        if (sucesso) {

            Files.copy(
                    Paths.get("output/resultado_questao08/part-r-00000"),
                    Paths.get("output/resultado-08.txt"),
                    StandardCopyOption.REPLACE_EXISTING
            );

            System.out.println("Resultado salvo em output/resultado-08.txt");
        }

        System.exit(sucesso ? 0 : 1);
    }
}