import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class JSONBookParser {

    public Book parse(String path) throws FileNotFoundException {
        Gson gson = new Gson();
        try {
            JsonReader reader = new JsonReader(new FileReader(path));
            HashMap<String, Object> map = gson.fromJson(reader, HashMap.class);
            ArrayList<LinkedTreeMap<String, Object>> reviews = (ArrayList)map.get("reviews");
            String title = (String)map.get("title");

            List<Review> ReviewList = new LinkedList<>();
            for(LinkedTreeMap<String, Object> r : reviews){
                ReviewList.add(new Review((String) r.get("id"), (String)r.get("link"), (String)r.get("title"),
                        (String)r.get("text"), (Double) r.get("rating"), (String)r.get("author"), (String)r.get("date")));
            }
           return new Book(title, ReviewList);

        } catch (FileNotFoundException e) {
            return null;
        }
    }
}
