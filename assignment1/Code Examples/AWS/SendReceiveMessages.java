import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import java.util.Date;
import java.util.List;
 
// snippet-end:[sqs.java2.send_recieve_messages.import]
// snippet-start:[sqs.java2.send_recieve_messages.main]
public class SendReceiveMessages
{
    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();
 
    public static void main(String[] args)
    {
       SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();
 
 
        try {
           CreateQueueRequest request = CreateQueueRequest.builder()
                 .queueName(QUEUE_NAME)
                 .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
           throw e;
 
        }
 
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
              .queueName(QUEUE_NAME)
              .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
 
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("hello world")
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
 
 
        // Send multiple messages to the queue
        SendMessageBatchRequest send_batch_request = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(
                        SendMessageBatchRequestEntry.builder()
                        .messageBody("Hello from message 1")
                        .id("msg_1")
                        .build()
                        ,
                        SendMessageBatchRequestEntry.builder()
                        .messageBody("Hello from message 2")
                        .delaySeconds(10)
                        .id("msg_2")
                        .build())
                .build();
        sqs.sendMessageBatch(send_batch_request);
 
        // receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
              .queueUrl(queueUrl)
              .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
 
        // delete messages from the queue
        for (Message m : messages) {
           DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                 .queueUrl(queueUrl)
                 .receiptHandle(m.receiptHandle())
                 .build();
            sqs.deleteMessage(deleteRequest);
        }
    }
}