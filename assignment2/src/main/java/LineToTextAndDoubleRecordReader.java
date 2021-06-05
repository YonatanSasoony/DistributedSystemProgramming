import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;

public class LineToTextAndDoubleRecordReader extends RecordReader<Text, DoubleWritable> {

    protected LineRecordReader reader;
    protected Text key;
    protected DoubleWritable value;

    LineToTextAndDoubleRecordReader() {
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
            value = new DoubleWritable(Double.parseDouble(toks[1]));
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
    public DoubleWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
}

