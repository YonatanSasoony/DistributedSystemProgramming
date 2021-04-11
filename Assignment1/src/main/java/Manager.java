import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {

    public static void main(String[] args) {
        System.out.println("Hello Manager");
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        String requestQueueUrl = AWSHelper.queueUrl(Defs.MANAGER_REQUEST_QUEUE_NAME);

        boolean isTerminated = false;
        while (!isTerminated) {
            // receive messages from the queue
            List<Message> responseMessages = null;
            while ((responseMessages = AWSHelper.receiveMessages(requestQueueUrl)) == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (Message m : responseMessages) {
                // msg = <localApplicationID>:<bucket>:<key>:<n>:<terminate> - what else?
                //TODO: create a message class?
                String[] content = m.body().split(":");
                String localAppId = content[0];
                String bucket = content[1];
                String key = content[2];
                Integer n = Integer.getInteger(content[3]);
                Boolean terminate = Boolean.getBoolean(content[4]);

                Book book = JSONBookParser.parse(AWSHelper.downloadFromS3(bucket, key));
                int reviewsNum = book.getReviews().size();
                int numOfWorkers = reviewsNum / n ; //TODO: +1?
                int numOfWorkersToAdd = 1; // TODO: calculate

                for (int i=0; i < numOfWorkersToAdd; i++) {
                    AWSHelper.createWorkerInstance();
                }
                executor.execute(new ManagerTask(localAppId, book, bucket, key));
                if (terminate) {
                    executor.shutdown();
                    isTerminated = true;
                }
            }
        }

        while (true) {
            try {
                if (executor.awaitTermination(2, TimeUnit.SECONDS)) break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
