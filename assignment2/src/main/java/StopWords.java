import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Pattern;

public class StopWords {
    private static StopWords stopWords = null;
    private static Pattern hebrewPattern = Pattern.compile("^[\"א-ת']*$");
    private HashSet<String> set;

    private StopWords(){
        set = new HashSet<>();
        fillSet();
    }

    public static StopWords getInstance(){
        if(stopWords == null)
            stopWords = new StopWords();
        return stopWords;
    }

    public boolean contains(String[] words) {
        for (String word : words) {
            if (contains(word)) {
                return true;
            }
        }
        return false;
    }

    private boolean legal(String word) {
        return  word.length() > 1 && hebrewPattern.matcher(word).find();
    }

    public boolean contains(String word){
        return !legal(word) || this.set.contains(word);
    }

    private void fillSet() {
        String words = "״\n" +
                "׳\n" +
                "של\n" +
                "רב\n" +
                "פי\n" +
                "עם\n" +
                "עליו\n" +
                "עליהם\n" +
                "על\n" +
                "עד\n" +
                "מן\n" +
                "מכל\n" +
                "מי\n" +
                "מהם\n" +
                "מה\n" +
                "מ\n" +
                "למה\n" +
                "לכל\n" +
                "לי\n" +
                "לו\n" +
                "להיות\n" +
                "לה\n" +
                "לא\n" +
                "כן\n" +
                "כמה\n" +
                "כלי\n" +
                "כל\n" +
                "כי\n" +
                "יש\n" +
                "ימים\n" +
                "יותר\n" +
                "יד\n" +
                "י\n" +
                "זה\n" +
                "ז\n" +
                "ועל\n" +
                "ומי\n" +
                "ולא\n" +
                "וכן\n" +
                "וכל\n" +
                "והיא\n" +
                "והוא\n" +
                "ואם\n" +
                "ו\n" +
                "הרבה\n" +
                "הנה\n" +
                "היו\n" +
                "היה\n" +
                "היא\n" +
                "הזה\n" +
                "הוא\n" +
                "דבר\n" +
                "ד\n" +
                "ג\n" +
                "בני\n" +
                "בכל\n" +
                "בו\n" +
                "בה\n" +
                "בא\n" +
                "את\n" +
                "אשר\n" +
                "אם\n" +
                "אלה\n" +
                "אל\n" +
                "אך\n" +
                "איש\n" +
                "אין\n" +
                "אחת\n" +
                "אחר\n" +
                "אחד\n" +
                "אז\n" +
                "אותו\n" +
                "־\n" +
                "^\n" +
                "?\n" +
                ";\n" +
                ":\n" +
                "1\n" +
                "2\n" +
                "3\n" +
                "4\n" +
                "5\n" +
                "6\n" +
                "7\n" +
                "8\n" +
                "9\n" +
                ".\n" +
                "-\n" +
                "*\n" +
                "\"\n" +
                "!\n" +
                "שלשה\n" +
                "בעל\n" +
                "פני\n" +
                ")\n" +
                "גדול\n" +
                "שם\n" +
                "עלי\n" +
                "עולם\n" +
                "מקום\n" +
                "לעולם\n" +
                "לנו\n" +
                "להם\n" +
                "ישראל\n" +
                "יודע\n" +
                "זאת\n" +
                "השמים\n" +
                "הזאת\n" +
                "הדברים\n" +
                "הדבר\n" +
                "הבית\n" +
                "האמת\n" +
                "דברי\n" +
                "במקום\n" +
                "בהם\n" +
                "אמרו\n" +
                "אינם\n" +
                "אחרי\n" +
                "אותם\n" +
                "אדם\n" +
                "(\n" +
                "חלק\n" +
                "שני\n" +
                "שכל\n" +
                "שאר\n" +
                "ש\n" +
                "ר\n" +
                "פעמים\n" +
                "נעשה\n" +
                "ן\n" +
                "ממנו\n" +
                "מלא\n" +
                "מזה\n" +
                "ם\n" +
                "לפי\n" +
                "ל\n" +
                "כמו\n" +
                "כבר\n" +
                "כ\n" +
                "זו\n" +
                "ומה\n" +
                "ולכל\n" +
                "ובין\n" +
                "ואין\n" +
                "הן\n" +
                "היתה\n" +
                "הא\n" +
                "ה\n" +
                "בל\n" +
                "בין\n" +
                "בזה\n" +
                "ב\n" +
                "אף\n" +
                "אי\n" +
                "אותה\n" +
                "או\n" +
                "אבל\n" +
                "א";
        String[] wordsArr = words.split("\n");
        Collections.addAll(this.set, wordsArr);
    }
}
