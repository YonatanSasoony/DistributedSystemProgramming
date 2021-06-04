import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class ExtractCollocationsRecordReader extends BigramRecordReader {

    protected Text parseKey(String str) {
        String[] toks = str.split("\t"); // TODO \t?
        return new Text(toks[0]);
    }

    protected LongWritable parseValue(String str) throws IOException {
        String[] toks = str.split("\t"); // TODO \t?
        return new LongWritable(Long.parseLong(toks[1]));
    }

}
