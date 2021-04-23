import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {

    private static final String in = Defs.internalDelimiter;
    private static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    private static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();

    public static void main(String[] args) {
        while (true) {
            Message message = AWSHelper.receiveSingleMessage(Defs.WORKER_REQUEST_QUEUE_NAME);
            System.out.println("worker got message");
            String response = analyzeAndSendMessage(message);
            // delete message only after finished working on task-
            // if failed, the message wont be deleted and other worker will handle this task instead
            if (response != null) {
                AWSHelper.deleteMessage(Defs.WORKER_REQUEST_QUEUE_NAME, message);
                System.out.println("worker sent response: " + response);
            }
        }
    }

    private static String analyzeAndSendMessage(Message message){
        String responseMessage = null;
        try {
            // request = <LocalAppID><inputNum><operation><reviewID><review>
            String[] parsedMessage = message.body().split(in);
            String localAppID = parsedMessage[0];
            String inputNum = parsedMessage[1];
            String operation = parsedMessage[2];
            String reviewID = parsedMessage[3];
            String review = parsedMessage[4];

            String output = "";
            if (operation.equals(Defs.SENTIMENT_ANALYSIS_OPERATION)) {
                output = sentimentAnalysisHandler.findSentiment(review);
            }
            if (operation.equals(Defs.ENTITY_RECOGNITION_OPERATION)) {
                output = namedEntityRecognitionHandler.findEntities(review);
            }
            responseMessage = localAppID + in + inputNum + in + operation + in + reviewID + in + output;
            AWSHelper.sendMessage(Defs.WORKER_RESPONSE_QUEUE_NAME+localAppID+inputNum, responseMessage);
        }catch (Exception e){
            e.printStackTrace();
        }

        return responseMessage;
    }
}
