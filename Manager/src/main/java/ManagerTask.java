import software.amazon.awssdk.services.sqs.model.Message;

import java.util.*;

public class ManagerTask implements Runnable {

    private final String localAppId;
    private final String inputNum;
    private final Product product;
    private final String bucket;
    private static final String in = Defs.internalDelimiter;

    public ManagerTask(String localAppId, String inputNum, Product product, String bucket) {
        this.localAppId = localAppId;
        this.inputNum = inputNum;
        this.product = product;
        this.bucket = bucket;
    }

    public void run() {
        System.out.println("manager task init");
        int totalTasks = 0;
        for (Review review : this.product.reviews()) {
            // request = <localAppId><inputNum><operation><reviewId><review>
            List<String> tasks = new ArrayList<>();
            tasks.add(this.localAppId + in + this.inputNum + in + Defs.SENTIMENT_ANALYSIS_OPERATION + in + review.id() + in + review.text());
            tasks.add(this.localAppId + in + this.inputNum + in + Defs.ENTITY_RECOGNITION_OPERATION + in + review.id() + in + review.text());
            AWSHelper.sendMessages(Defs.WORKER_REQUEST_QUEUE_NAME, tasks);
            totalTasks += 2;
        }

        int tasksCompleted = 0;
        String summaryMsg = "";
        System.out.println("sent all tasks to workers, now waiting");
        while (tasksCompleted < totalTasks) {
            // receive messages from the queue
            List<Message> responseMessages = AWSHelper.receiveMessages(Defs.WORKER_RESPONSE_QUEUE_NAME);
            for (Message msg : responseMessages) {
                // response = <LocalAppID><inputNum><operation><reviewID><output>
                String[] parsedMessage = msg.body().split(in);
                String localAppId = parsedMessage[0];
                String inputNum = parsedMessage[1];
                String operation = parsedMessage[2];
                String reviewId = parsedMessage[3];
                String output = parsedMessage[4];

                if (this.localAppId.equals(localAppId) && this.inputNum.equals(inputNum)){ // relevant response
                    // need - id, rating, link, 2 outputs for each review
                    String rating = this.product.ratingFromReviewId(reviewId).toString();
                    String link = this.product.linkFromReviewId(reviewId);
                    // summaryMsg - (<reviewId><rating><link><operation><output>ex)*<description>
                    summaryMsg += reviewId + in + rating + in + link + in + operation + in + output + Defs.externalDelimiter;
                    tasksCompleted++;
                    AWSHelper.deleteMessage(Defs.WORKER_RESPONSE_QUEUE_NAME, msg);
                    System.out.println("tasks completed: "+tasksCompleted);
                }
            }
        }

        summaryMsg += product.title();
        System.out.println("manager task creating summary");
        String key = "Summary" + System.currentTimeMillis();
        AWSHelper.uploadContentToS3(this.bucket, key, summaryMsg);
        // response - <localApplicationID><inputNum><bucket><key>
        String response = this.localAppId + in + inputNum+ in + this.bucket + in + key;
        AWSHelper.sendMessage(Defs.MANAGER_RESPONSE_QUEUE_NAME, response);
        System.out.println("manager task uploaded & sent summary");

        // TODO: IMPORTANT: If a worker stops working unexpectedly before finishing its work on a message,
        // then some other worker should be able to handle that message.
    }
}
