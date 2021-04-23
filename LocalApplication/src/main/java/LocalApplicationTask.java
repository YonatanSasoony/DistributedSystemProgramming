import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalApplicationTask implements Runnable {

    private String outputFilePath;
    private String bucket;
    private String key;

    public LocalApplicationTask(String outputFilePath, String bucket, String key) {
        this.outputFilePath = outputFilePath;
        this.bucket = bucket;
        this.key = key;
    }

    public void run() {
        InputStream stream = AWSHelper.downloadFromS3(bucket, key);
        String summaryMsg = new BufferedReader(new InputStreamReader(stream))
                .lines().collect(Collectors.joining(""));

        System.out.println("local creating html");
        //summaryMsg - (<reviewId><rating><link><operation><output>ex)^*
        Map<String, String[]> reviewsOutputMap = new HashMap<>();
        String[] workersOutputs = summaryMsg.split(Defs.externalDelimiter);
        for (int i = 0; i < workersOutputs.length; i++) {
            String[] outputContent = workersOutputs[i].split(Defs.internalDelimiter);
            String reviewId = outputContent[0];
            String rating = outputContent[1];
            String link = outputContent[2];
            String operation = outputContent[3];
            String output = outputContent[4];
            if (!reviewsOutputMap.containsKey(reviewId)) {
                reviewsOutputMap.put(reviewId, new String[4]);
            }
            String[] outputs = reviewsOutputMap.get(reviewId);
            outputs[0] = rating;
            outputs[1] = link;
            if (operation.equals(Defs.SENTIMENT_ANALYSIS_OPERATION)) {
                outputs[2] = output;
            } else {
                outputs[3] = output;
            }
        }
        String htmlString = createHTMLString(reviewsOutputMap);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath + ".html"));
            writer.write(htmlString);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final String templateHTML =
            "<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<head>\n" +
                    "<meta charset=UTF-8\">\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "$body" +
                    "</body>\n" +
                    "</html>";

    private static String createHTMLString(Map<String,String[]> reviewsMap) {
        //reviewMap - id -> [rating, link, sentiment, entity]
        String htmlString = templateHTML;
        String body = "<h2>"+"Reviews from Amazon"+"</h2>\n";
        body += "<ul>\n";
        for (String reviewId : reviewsMap.keySet()) {
            String[] review = reviewsMap.get(reviewId);
            String rating = review[0];
            String link = review[1];
            String sentiment = review[2];
            String entity = review[3];
            body += "<li>\n";
            body += "<a href=\"" + link+"\"";
            String color = colorFromSentiment(sentiment);
            body += " style=\"color:"+color+";\"> ";
            body += link + "</a>\n";
            body += "<p>" + entity +"</p>\n";
            String sarcasm = getSarcasm(rating, sentiment);
            body += "<p>" + sarcasm +"</p>\n";
            body += "</li>\n";
        }
        body += "</ul>\n";
        htmlString = htmlString.replace("$body", body);
        return htmlString;
    }

    private static String colorFromSentiment(String sentiment) {
        switch (sentiment) {
            case "1":
                return "darkRed";
            case "2":
                return "Red";
            case "3":
                return "Black";
            case "4":
                return "LightGreen";
            case "5":
                return "DarkGreen";
        }
        return "White";
    }

    private static String getSarcasm(String rating, String sentiment) {
        Double ratingVal = Double.parseDouble(rating);
        Double sentimentVal = Double.parseDouble(sentiment);
        return Math.abs(ratingVal - sentimentVal) >= 2 ? "Sarcasm" : "No Sarcasm";
    }
}
