import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public class Worker {

    private static final String in = Defs.internalDelimiter;

    public static void main(String[] args) {

        //TODO: infinite loop?
        while (true) {
            Message message = AWSHelper.receiveSingleMessage(Defs.WORKER_REQUEST_QUEUE_NAME);
            System.out.println("worker got message");
            String response = analyzeMessage(message);
            AWSHelper.deleteMessage(Defs.WORKER_REQUEST_QUEUE_NAME, message);
            AWSHelper.sendMessage(Defs.WORKER_RESPONSE_QUEUE_NAME, response);
            System.out.println("worker sent response: " + response);
        }
    }

    private static String analyzeMessage(Message message){
        String responseMessage = null;
        try {
            // msg = <LocalAppID><operation><reviewID><review>
            String[] parsedMessage = message.body().split(in);
            String localAppID = parsedMessage[0];
            String operation = parsedMessage[1];
            String reviewID = parsedMessage[2];
            String review = parsedMessage[3];

            String output = "";
            if (operation.equals(Defs.SENTIMENT_ANALYSIS_OPERATION)) {
                SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
                output = sentimentAnalysisHandler.findSentiment(review);
            }
            if (operation.equals(Defs.ENTITY_RECOGNITION_OPERATION)) {
//                NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
                output = "OBAMA;PERSON";//namedEntityRecognitionHandler.findEntities(review); //TODO:
            }
            responseMessage = localAppID + in + operation + in + reviewID + in + output;

        }catch (Exception e){
            e.printStackTrace();
        }
        return responseMessage;
    }



}
