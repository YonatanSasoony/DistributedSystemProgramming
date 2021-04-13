import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {

    public static void main(String[] args) {
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        boolean isTerminated = false;
        while (!isTerminated) {
            // receive messages from the queue
            List<Message> requestMessages = AWSHelper.receiveMessages(Defs.MANAGER_REQUEST_QUEUE_NAME);
            if (requestMessages.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (Message msg : requestMessages) {
                // msg = <localApplicationID>:<bucket>:<key>:<n>:<terminate> - what else?
                System.out.println("manager received message: "+msg.body());
                String[] content = msg.body().split(":");
                String localAppId = content[0];
                String bucket = content[1];
                String key = content[2];
                Integer n = Integer.parseInt(content[3]);
                Boolean terminate = Boolean.parseBoolean(content[4]);

                Book book = JSONBookParser.parse(AWSHelper.downloadFromS3(bucket, key));
                System.out.println(book.getTitle());
                int reviewsNum = book.getReviews().size();
                int requiredWorkers = (int)(Math.ceil(reviewsNum / n)); // m
                int activeWorkers = AWSHelper.activeWorkers(); //TODO: synchronize?! // k
                int numOfWorkersToAdd = requiredWorkers - activeWorkers;
                AWSHelper.createWorkerInstances(numOfWorkersToAdd);
                AWSHelper.deleteMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, msg);
                executor.execute(new ManagerTask(localAppId, book, bucket));

                if (terminate) {
                    executor.shutdown();
                    isTerminated = true;
                }
            }
        }

        // wait for threads to finish
        while (true) {
            try {
                if (executor.awaitTermination(2, TimeUnit.SECONDS)) break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
