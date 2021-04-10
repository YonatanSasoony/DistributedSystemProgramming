import java.util.ArrayList;
import java.util.List;

public class ManagerTask implements Runnable {

    private final String localAppId;
    private final Book book;
    private final String bucket;
    private final String key;

    public ManagerTask(String localAppId, Book book, String bucket, String key) {
        this.localAppId = localAppId;
        this.book = book;
        this.bucket = bucket;
        this.key = key;
    }

    public void run() {
        for (Review review : this.book.getReviews()) {
            // task = LocalAppID + ":" + operation + ":" + reviewID + ":" + review
            List<String> tasks = new ArrayList<>();
            tasks.add(this.localAppId + ":" + Defs.SENTIMENT_ANALYSIS_OPERATION + ":" + review.getId() + ":" + review.getText());
            tasks.add(this.localAppId + ":" + Defs.ENTITY_RECOGNITION_OPERATION + ":" + review.getId() + ":" + review.getText());
            String requestQueueUrl = AWSHelper.queueUrl(Defs.WORKER_REQUEST_QUEUE_NAME);
            AWSHelper.sendMessages(requestQueueUrl, tasks);
        }

        // IMPORTANT: If a worker stops working unexpectedly before finishing its work on a message,
        // then some other worker should be able to handle that message.
        //TODO: create summary file, upload to S3, send url to sqs
        String summary = "";
        AWSHelper.uploadToS3(bucket, key, summary);

        String summaryUrl = "";
        String body = "ID" + " :" + summaryUrl;
        AWSHelper.sendMessage(AWSHelper.queueUrl(Defs.MANAGER_RESPONSE_QUEUE_NAME), body);

    }
}
