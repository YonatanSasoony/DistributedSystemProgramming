import software.amazon.awssdk.services.sqs.model.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {

    public static void main(String[] args) {
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        boolean isTerminated = false;
        int totalWorkers = 0;
        int activeWorkers = 0;
        while (!isTerminated) {
            // receive messages from the queue
            List<Message> requestMessages = AWSHelper.receiveMessages(Defs.MANAGER_REQUEST_QUEUE_NAME);
            System.out.println("request messages size: "+requestMessages.size());
            for (Message msg : requestMessages) {
                System.out.println("manager received message: "+msg.body());
                if (msg.body().equals(Defs.TERMINATE_MESSAGE)) {
                    executor.shutdown();
                    isTerminated = true;
                    AWSHelper.deleteMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, msg);
                    break;
                }
                // request - <localApplicationID><inputNum><bucket><key><n>
                String[] content = msg.body().split(Defs.internalDelimiter);
                String localAppId = content[0];
                String inputNum = content[1];
                String bucket = content[2];
                String key = content[3];
                Integer n = Integer.parseInt(content[4]);

                Map<String, Review> reviewsMap = JSONParser.parse(AWSHelper.downloadFromS3(bucket, key));
                int requiredWorkers = (int)(Math.ceil(reviewsMap.size() / n)); // m
                activeWorkers = AWSHelper.activeWorkers();
                int numOfWorkersToAdd = requiredWorkers - activeWorkers;
                if (numOfWorkersToAdd > 0) {
                    totalWorkers += numOfWorkersToAdd;
                }
                AWSHelper.createWorkerInstances(numOfWorkersToAdd);
                executor.execute(new ManagerTask(localAppId, inputNum, reviewsMap, bucket));
                AWSHelper.deleteMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, msg);

            }
            //check if there are missing active workers
            activeWorkers = AWSHelper.activeWorkers();
            if (!isTerminated && activeWorkers < totalWorkers) {
                AWSHelper.createWorkerInstances(totalWorkers - activeWorkers);
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
        AWSHelper.shutdownWorkers();
    }
}
