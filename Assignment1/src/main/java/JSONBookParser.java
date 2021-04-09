import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.*;

public class JSONBookParser {

//    public static Book parse(String path) {
//        Gson gson = new Gson();
//        try {
//            JsonReader reader = new JsonReader(new FileReader(path));
//            return gson.fromJson(reader, Book.class);
//
//        } catch (Exception e) {
//            System.out.println("failed to parse json: "+e);
//            return null;
//        }
//    }

        public static Book parse(InputStream stream) {
            try {
            JsonParser parser = new JsonParser();
            JsonObject jo = parser.parse(new InputStreamReader(stream)).getAsJsonObject();
            String bookTitle = jo.getAsJsonPrimitive("title").getAsString();
            JsonArray jsonReviews = jo.getAsJsonArray("reviews");
            List<Review> parsedReviews = new ArrayList<>();
            for (Object review : jsonReviews) {
                String id = ((JsonObject) review).getAsJsonPrimitive("id").getAsString();
                String link = ((JsonObject) review).getAsJsonPrimitive("link").getAsString();
                String title = ((JsonObject) review).getAsJsonPrimitive("title").getAsString();
                String text = ((JsonObject) review).getAsJsonPrimitive("text").getAsString();
                Double rating = ((JsonObject) review).getAsJsonPrimitive("rating").getAsDouble();
                String author = ((JsonObject) review).getAsJsonPrimitive("author").getAsString();
                String date = ((JsonObject) review).getAsJsonPrimitive("date").getAsString();
                parsedReviews.add(new Review(id, link, title, text, rating, author, date));
            }

            Book b = new Book(bookTitle, parsedReviews);
            System.out.println("book parsed");
            return b;
        } catch (Exception e) {
            System.out.println("failed parse stream");
            System.out.println("failed to parse json: "+e);
            return null;
        }
    }

    public static Book parse(byte[] bytes) {
        return parse(new String(bytes));
    }

    public static Book parse(String json) {
        Gson gson = new GsonBuilder()
                .setLenient()
                .create();
        try {
            Book b = gson.fromJson(json, Book.class);
            System.out.println("book parsed");
            return b;
        } catch (Exception e) {
            System.out.println("failed parse bytes");
            System.out.println("failed to parse json: "+e);
            return null;
        }
//        try {
//            JsonParser parser = new JsonParser();
//            JsonObject jo = parser.parse(json).getAsJsonObject();
//            String bookTitle = jo.getAsJsonPrimitive("title").getAsString();
//            JsonArray jsonReviews = jo.getAsJsonArray("reviews");
//            List<Review> parsedReviews = new ArrayList<>();
//            for (Object review : jsonReviews) {
//                String id = ((JsonObject) review).getAsJsonPrimitive("id").getAsString();
//                String link = ((JsonObject) review).getAsJsonPrimitive("link").getAsString();
//                String title = ((JsonObject) review).getAsJsonPrimitive("title").getAsString();
//                String text = ((JsonObject) review).getAsJsonPrimitive("text").getAsString();
//                Double rating = ((JsonObject) review).getAsJsonPrimitive("rating").getAsDouble();
//                String author = ((JsonObject) review).getAsJsonPrimitive("author").getAsString();
//                String date = ((JsonObject) review).getAsJsonPrimitive("date").getAsString();
//                parsedReviews.add(new Review(id, link, title, text, rating, author, date));
//            }
//
//            Book b = new Book(bookTitle, parsedReviews);
//            System.out.println("book parsed");
//            return b;
//        } catch (Exception e) {
//            System.out.println("failed parse book");
//            System.out.println("failed to parse json: "+e);
//            return null;
//        }
    }

    private static Book parse(Map<String, Object> map) {
        ArrayList<LinkedTreeMap<String, Object>> reviews = (ArrayList)map.get("reviews");
        String title = (String)map.get("title");

        List<Review> ReviewList = new LinkedList<>();
        for(LinkedTreeMap<String, Object> r : reviews){
            ReviewList.add(new Review((String) r.get("id"), (String)r.get("link"), (String)r.get("title"),
                    (String)r.get("text"), (Double) r.get("rating"), (String)r.get("author"), (String)r.get("date")));
        }
        return new Book(title, ReviewList);
    }
}
