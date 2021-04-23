import com.google.gson.*;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JSONParser {

    public static Map<String, Review> parse(InputStream stream) {
        Gson gson = new Gson();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            Map<String, Review> reviewsMap = new HashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                JsonReader jReader = new JsonReader(new StringReader(line));
                Product p = gson.fromJson(jReader, Product.class);
                for (Review r : p.reviews()) {
                    reviewsMap.put(r.id(), r);
                }
            }
            return reviewsMap;
        } catch (Exception e) {
            System.out.println("failed to parse json: "+e);
            return null;
        }
    }
}