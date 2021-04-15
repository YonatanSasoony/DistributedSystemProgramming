import com.google.gson.*;
import com.google.gson.stream.JsonReader;

import java.io.InputStream;
import java.io.InputStreamReader;

public class JSONProductParser {

    public static Product parse(InputStream stream) {
        Gson gson = new Gson();
        try {
            JsonReader reader = new JsonReader(new InputStreamReader(stream));
            return gson.fromJson(reader, Product.class);
        } catch (Exception e) {
            System.out.println("failed to parse json: "+e);
            return null;
        }
    }
}