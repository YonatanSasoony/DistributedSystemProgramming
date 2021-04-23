import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        String s = "bla#bla";
        String[] x = s.split("#");
        int gx = 3;

//        try {
//            Gson gson = new Gson();
//
//            BufferedReader reader = new BufferedReader(new FileReader("i1.txt"));
//            List<Product> list = new ArrayList<>();
//            String line;
//            while ((line = reader.readLine()) != null) {
//                JsonReader jreader = new JsonReader(new StringReader(line));
//                Product p = gson.fromJson(jreader, Product.class);
//                list.add(p);
//            }
//            int x = 5;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
}
