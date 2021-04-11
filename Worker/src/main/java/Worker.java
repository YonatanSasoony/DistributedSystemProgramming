import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

public class Worker {


    public static void main(String[] args) {
        //TODO: infinite loop?
        Message message = AWSHelper.receiveSingleMessage(AWSHelper.queueUrl(Defs.WORKER_REQUEST_QUEUE_NAME));
        AWSHelper.sendMessage(Defs.MANAGER_RESPONSE_QUEUE_NAME, analyzeMessage(message));
    }

    private static String analyzeMessage(Message message){
        String responseMessage = null;
        try {
            // msg = <LocalAppID>:<operation>:<reviewID>:<review>
            String[] parsedMessage = message.body().split(":");
            String shortLocalAppID = parsedMessage[0];
            String operation = parsedMessage[1];
            String reviewID = parsedMessage[2];
            String review = parsedMessage[3];

            String output = "";
            if (operation.equals(Defs.SENTIMENT_ANALYSIS_OPERATION)) {
                SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
                output = sentimentAnalysisHandler.findSentiment(review);
            }
            if (operation.equals(Defs.ENTITY_RECOGNITION_OPERATION)) {
                NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
                List<String> entities = namedEntityRecognitionHandler.findEntities(review);
                output = entities.toString(); // TODO: VERIFY
            }
            responseMessage = shortLocalAppID + ":" + operation + " :" + reviewID + " :" + output;

        }catch (Exception e){
            e.printStackTrace();
        }
        return responseMessage;
    }



}
