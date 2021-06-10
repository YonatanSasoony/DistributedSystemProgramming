import java.io.*;
import java.util.Scanner;

public class TopTenExtractor {
    public static void main(String[] args) throws FileNotFoundException {

        String filePath = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\npmis.txt";
        BufferedReader reader;
        try {
            FileWriter output = new FileWriter("top10.txt");
            reader = new BufferedReader(new FileReader(filePath));

            String line = reader.readLine();
            while (line != null) {
                line = reader.readLine();
                if (line == null)
                    break;
                if (line.contains("Decade")) {
                    output.write(line+"\n");
                    for (int i =0; line != null && i < 10;i++) {
                        line = reader.readLine();
                        if (line == null)
                            break;
                        output.write(line+"\n");
                    }
                }
            }
            reader.close();
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
