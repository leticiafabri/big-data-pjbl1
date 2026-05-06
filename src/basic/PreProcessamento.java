package basic;

public class PreProcessamento {

    // Verifica se a linha é cabeçalho
    public static boolean isHeader(String line) {
        return line.startsWith("country_or_area");
    }

    // Verifica se a linha possui dados válidos
    public static boolean isValid(String[] fields) {

        // Dataset deve possuir 10 colunas
        if (fields.length < 10) {
            return false;
        }

        // Verifica campos obrigatórios
        return !fields[0].trim().isEmpty() && // country
                !fields[1].trim().isEmpty() && // year
                !fields[4].trim().isEmpty() && // flow
                !fields[9].trim().isEmpty();   // category
    }
}