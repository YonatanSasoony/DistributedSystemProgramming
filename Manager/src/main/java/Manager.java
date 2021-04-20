import software.amazon.awssdk.services.sqs.model.*;
import java.util.List;
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
                    break;
                }
                // request - <localApplicationID><inputNum><bucket><key><n>
                String[] content = msg.body().split(Defs.internalDelimiter);
                String localAppId = content[0];
                String inputNum = content[1];
                String bucket = content[2];
                String key = content[3];
                Integer n = Integer.parseInt(content[4]);

                Product product = JSONProductParser.parse(AWSHelper.downloadFromS3(bucket, key));
                System.out.println(product.title());
                int reviewsNum = product.reviews().size();
                int requiredWorkers = (int)(Math.ceil(reviewsNum / n)); // m
                activeWorkers = AWSHelper.activeWorkers();
                int numOfWorkersToAdd = requiredWorkers - activeWorkers;
                if (numOfWorkersToAdd > 0) {
                    totalWorkers += numOfWorkersToAdd;
                }
                AWSHelper.createWorkerInstances(numOfWorkersToAdd);
                try {
                    executor.execute(new ManagerTask(localAppId, inputNum, product, bucket));
                    AWSHelper.deleteMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, msg);
                } catch (RejectedExecutionException e) {
                    // executor is full- open new manager
                    // need to change receive message visibility time out and max number
                    //ChangeMessageVisibility to make the failed processed massage visible to different managers..
                }
            }
            //check if there are missing active workers
            activeWorkers = AWSHelper.activeWorkers();
            if (activeWorkers < totalWorkers) {
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
