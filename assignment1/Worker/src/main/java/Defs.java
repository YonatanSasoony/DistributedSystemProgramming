import software.amazon.awssdk.regions.Region;

public class Defs {
    static final Region REGION = Region.US_EAST_1;
    static final Region GLOBAL_REGION = Region.AWS_GLOBAL;

    static final String MANAGER_REQUEST_QUEUE_NAME = "ManagerRequestQueue";
    static final String MANAGER_RESPONSE_QUEUE_NAME = "ManagerResponseQueue";
    static final String WORKER_REQUEST_QUEUE_NAME = "WorkerRequestQueue";
    static final String WORKER_RESPONSE_QUEUE_NAME = "WorkerResponseQueue";

    static final String ENTITY_RECOGNITION_OPERATION = "EntityRecognition";
    static final String SENTIMENT_ANALYSIS_OPERATION = "SentimentAnalysis";

    static final String internalDelimiter = "::";
    static final String externalDelimiter = "<:>";
}
