package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Questao03 {

//Transações por CATEGORIA

    // MAP
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        private Text categoryText = new Text();

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

            // Coluna da categoria
            String category = fields[9];

            // Ignora categoria vazia
            if (category.trim().isEmpty()) {
                return;
            }

            categoryText.set(category);

            con.write(categoryText, one);
        }
    }

    // REDUCE
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            con.write(key, new IntWritable(sum));
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job j = Job.getInstance(conf, "Questao03");

        j.setJarByClass(Questao03.class);

        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // Arquivo de entrada
        FileInputFormat.addInputPath(j,
                new Path("in/operacoes_comerciais_inteira - cópia.csv"));

        // Pasta temporária de saída
        FileOutputFormat.setOutputPath(j,
                new Path("output/resultado_questao03"));

        boolean sucesso = j.waitForCompletion(true);

        // Copia resultado final para TXT
        if (sucesso) {

            Files.copy(
                    Paths.get("output/resultado_questao03/part-r-00000"),
                    Paths.get("output/resultado-03.txt"),
                    StandardCopyOption.REPLACE_EXISTING
            );

            System.out.println("Resultado salvo em output/resultado-03.txt");
        }

        System.exit(sucesso ? 0 : 1);
    }
}