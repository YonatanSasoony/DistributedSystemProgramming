import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NamedEntityRecognitionHandler {
    private StanfordCoreNLP NERPipeline;
    public NamedEntityRecognitionHandler() {
        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner"); //TODO: set
        NERPipeline = new StanfordCoreNLP(props);
    }
    public String findEntities(String review){
        List<String> types = new ArrayList<>();
        types.add("PERSON");
        types.add("LOCATION");
        types.add("ORGANIZATION");

        String entities = "";
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);
        // run all Annotators on this text
        NERPipeline.annotate(document);
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                System.out.println("word in entity: "+word);
                String ne = token.get(NamedEntityTagAnnotation.class);
                System.out.println("ne in entity: "+ne);

//                System.out.println("\t-" + word + ":" + ne);
                if (types.contains(ne)) {
                    entities += word + ";" + ne + "@";
                }
            }
        }

        return entities.length() == 0 ? "null" : entities.substring(0, entities.length() - 1); // remove the last char
    }
}
