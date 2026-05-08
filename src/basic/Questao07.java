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

public class Questao07 {

    // MAP
    public static class Map extends Mapper<LongWritable, Text, Text, MediaWritable> {

        private Text yearText = new Text();

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            //remove cabeçalho
            if (PreProcessamento.isHeader(line)) {
                return;
            }

            String[] fields = line.split(";");

            //trata os dados faltantes
            if (!PreProcessamento.isValid(fields)) {
                return;
            }

            String country = fields[0];
            String year = fields[1];
            String flow = fields[4];
            String amountString = fields[8];

            //apenas Brasil
            if (!country.equalsIgnoreCase("Brazil")) {
                return;
            }

            //apenas Export
            if (!flow.equalsIgnoreCase("Export")) {
                return;
            }

            //ignora amount vazio
            if (amountString.trim().isEmpty()) {
                return;
            }

            amountString = amountString.replace(",", ".");

            double amount;

            try {
                amount = Double.parseDouble(amountString);
            } catch (NumberFormatException e) {
                return;
            }

            yearText.set(year);

            con.write(yearText,
                    new MediaWritable(amount, 1));
        }
    }

    // COMBINER
    public static class Combiner extends Reducer<Text, MediaWritable, Text, MediaWritable> {

        public void reduce(Text key,
                           Iterable<MediaWritable> values,
                           Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            int quantidade = 0;

            for (MediaWritable val : values) {

                soma += val.getSoma();
                quantidade += val.getQuantidade();
            }

            con.write(key,
                    new MediaWritable(soma, quantidade));
        }
    }

    // REDUCE
    public static class Reduce extends Reducer<Text, MediaWritable, Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<MediaWritable> values,
                           Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            int quantidade = 0;

            for (MediaWritable val : values) {

                soma += val.getSoma();
                quantidade += val.getQuantidade();
            }

            double media = soma / quantidade;

            con.write(key,
                    new DoubleWritable(media));
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job j = Job.getInstance(conf, "Questao07");

        j.setJarByClass(Questao07.class);

        j.setMapperClass(Map.class);

        //combiner obrigatório
        j.setCombinerClass(Combiner.class);

        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MediaWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        //arquivo de entrada
        FileInputFormat.addInputPath(j,
                new Path("in/operacoes_comerciais_inteira.csv"));

        //pasta temporária de saída
        FileOutputFormat.setOutputPath(j,
                new Path("output/resultado_questao07"));

        boolean sucesso = j.waitForCompletion(true);

        //copia resultado final para TXT
        if (sucesso) {

            Files.copy(
                    Paths.get("output/resultado_questao07/part-r-00000"),
                    Paths.get("output/resultado-07.txt"),
                    StandardCopyOption.REPLACE_EXISTING
            );

            System.out.println("Resultado salvo em output/resultado-07.txt");
        }

        System.exit(sucesso ? 0 : 1);
    }
}