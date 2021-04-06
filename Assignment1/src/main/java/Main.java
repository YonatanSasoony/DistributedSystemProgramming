import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        System.out.println("HELLO");
        Gson gson = new Gson();
        try {
            JsonReader reader = new JsonReader(new FileReader("src/Input files/i1.txt"));
            HashMap<String, Object> map = gson.fromJson(reader, HashMap.class);
            ArrayList<LinkedTreeMap<String, String>> reviews = (ArrayList)map.get("reviews");
            String title = (String)map.get("title");


            System.out.println("HELLO");

        } catch (FileNotFoundException e) {
            System.out.println("KAKA");

        }
    }
}