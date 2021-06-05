import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.io.IOException;


public class LineToTextAndLongRecordReader extends RecordReader<Text, LongWritable> {

    protected LineRecordReader reader;
    protected Text key;
    protected LongWritable value;

    LineToTextAndLongRecordReader() {
        reader = new LineRecordReader();
        key = null;
        value = null;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        reader.initialize(split, context);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.nextKeyValue()) {
            String[] toks = reader.getCurrentValue().toString().split(Defs.lineDataDelimiter);
            key = new Text(toks[0]);
            value = new LongWritable(Long.parseLong(toks[1]));
            return true;
        } else {
            key = null;
            value = null;
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public LongWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
}

