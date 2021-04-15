import java.io.*;
import java.util.*;
import java.util.UUID;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.sqs.model.*;
import org.apache.commons.io.FileUtils;

public class LocalApplication {

    public static void main(String[] args) {
        final String localApplicationID = UUID.randomUUID().toString();
        final String in = Defs.internalDelimiter;
        final String ex = Defs.externalDelimiter;
        //check if manager is active on EC2 cloud, if not- start the manager node.
        AWSHelper.runManager();
        AWSHelper.initQueues();

        // S3 folder for uploading input files
        String bucket = AWSHelper.createBucket(localApplicationID);

        int N = 1;//TODO: args.length % 2 == 0 ? (args.length - 2) / 2 : (args.length - 1) / 2;
        int n = 3;
        boolean terminate = false;

        for (int i = 0; i<N; i++) {
            String key = args[i];
            AWSHelper.uploadFileTOS3(bucket, key, key);
            String body = localApplicationID + in + bucket + in + key + in + n + in + terminate; //TODO: send relevant output path
            AWSHelper.sendMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, body);
            System.out.println("local uploaded & sent file "+key);
        }

// **********************************************************

        String summaryMsg = null;
        while (summaryMsg == null) {
            // TODO: CHECK BUSY WAIT METHOD
            // receive messages from the queue
            List<Message> responseMessages = AWSHelper.receiveMessages(Defs.MANAGER_RESPONSE_QUEUE_NAME);
            for (Message msg : responseMessages) {
                // msg = <localApplicationID><bucket><key>
                String[] content = msg.body().split(in);
                String receivedID = content[0];
                String receivedBucket = content[1];
                String receivedKey = content[2];
                if (localApplicationID.equals(receivedID)) {
                    System.out.println("local received response from manager "+msg.body());
                    InputStream stream = AWSHelper.downloadFromS3(receivedBucket, receivedKey);
                    summaryMsg = new BufferedReader(new InputStreamReader(stream))
                            .lines().collect(Collectors.joining("")); // TODO: CHECK
                    AWSHelper.deleteMessage(Defs.MANAGER_RESPONSE_QUEUE_NAME, msg);
                    break;
                }
            }
        }


        System.out.println("local creating html");
        //summaryMsg - (<reviewId><rating><link><operation><output>)*; //TODO: get book title?
        Map<String, String[]> reviewsOutputMap = new HashMap<>();
        String[] workersOutputs = summaryMsg.split(ex);
        for (int i = 0; i< workersOutputs.length; i++) {
            String[] outputContent = workersOutputs[i].split(in);
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

        String outputFilePath = args[1];
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
            "<title>$title</title>\n" +
            "</head>\n" +
            "<body>\n" +
            "$body" +
            "</body>\n" +
            "</html>";

    private static String createHTMLString(Map<String,String[]> reviewsMap) {
        String htmlString = templateHTML;
        String title = "YONI AND YOSSY";
        String body = "<ul>\n";
        for (String reviewId : reviewsMap.keySet()) {
            String[] review = reviewsMap.get(reviewId);
            body += "<li>\n";
            body += review[1];
            body += "</li>\n";
        }
        body += "</ul>\n";
        htmlString = htmlString.replace("$title", title);
        htmlString = htmlString.replace("$body", body);
        return htmlString;
    }
}
