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
            ArrayList<LinkedTreeMap<String, Object>> reviews = (ArrayList)map.get("reviews");
            String title = (String)map.get("title");

            List<Review> ReviewList = new LinkedList<>();
            for(LinkedTreeMap<String, Object> r : reviews){
                ReviewList.add(new Review((String) r.get("id"), (String)r.get("link"), (String)r.get("title"),
                        (String)r.get("text"), (Double) r.get("rating"), (String)r.get("author"), (String)r.get("date")));
            }
            Book book = new Book(title, ReviewList);
            //System.out.println(book);

        } catch (FileNotFoundException e) {
            System.out.println("JSON parsing Failed");
        }
    }
}