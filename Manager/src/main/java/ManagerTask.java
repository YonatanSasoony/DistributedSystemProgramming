import software.amazon.awssdk.services.sqs.model.Message;

import java.util.*;

public class ManagerTask implements Runnable {

    private final String localAppId;
    private final Book book;
    private final String bucket;

    public ManagerTask(String localAppId, Book book, String bucket) {
        this.localAppId = localAppId;
        this.book = book;
        this.bucket = bucket;
    }

    public void run() {
        System.out.println("manager task init");
        for (Review review : this.book.getReviews()) {
            // task = localAppId + ":" + operation + ":" + reviewId + ":" + review
            List<String> tasks = new ArrayList<>();
            tasks.add(this.localAppId + ":" + Defs.SENTIMENT_ANALYSIS_OPERATION + ":" + review.getId() + ":" + review.getText());
            tasks.add(this.localAppId + ":" + Defs.ENTITY_RECOGNITION_OPERATION + ":" + review.getId() + ":" + review.getText());
            AWSHelper.sendMessages(Defs.WORKER_REQUEST_QUEUE_NAME, tasks);
        }

        int totalTasks = this.book.getReviews().size() * 2;
        int tasksCompleted = 0;
        String summaryMsg = "";
        System.out.println("sent all tasks to workers, now waiting");
        while (tasksCompleted < totalTasks) {
            // receive messages from the queue
            List<Message> responseMessages = AWSHelper.receiveMessages(Defs.WORKER_RESPONSE_QUEUE_NAME);
            if (responseMessages.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (Message msg : responseMessages) {
//                msg = <LocalAppID>:<operation>:<reviewID>:<output>
                String[] parsedMessage = msg.body().split(":");
                String localAppId = parsedMessage[0];
                String operation = parsedMessage[1];
                String reviewId = parsedMessage[2];
                String output = parsedMessage[3];

                if (this.localAppId.equals(localAppId)){
                    // need - id, rating, link, 2 outputs for each review
                    AWSHelper.deleteMessage(Defs.WORKER_RESPONSE_QUEUE_NAME, msg);
                    String rating = this.book.getRatingFromReviewId(reviewId).toString();
                    String link = this.book.getLinkFromReviewId(reviewId);
                    summaryMsg += reviewId + ":" + rating + ":" + link + ":" + operation + ":" + output + "\n";
                    tasksCompleted++;
                    System.out.println("tasks completed: "+tasksCompleted);
                }
            }
        }
        System.out.println("manager task creating summary");
        String key = "Summary" + System.currentTimeMillis();
        AWSHelper.uploadContentToS3(this.bucket, key, summaryMsg);
        String response = this.localAppId + ":" + this.bucket + ":" + key;
        AWSHelper.sendMessage(Defs.MANAGER_RESPONSE_QUEUE_NAME, response);
        System.out.println("manager task uploaded & sent summary");

        // TODO: IMPORTANT: If a worker stops working unexpectedly before finishing its work on a message,
        // then some other worker should be able to handle that message.
    }
}
