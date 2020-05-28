import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl.DummyReporter;

import org.apache.hadoop.util.Progress;
import scala.Tuple2;

import java.io.IOException;

public class MultipleAvroOutputsFormat<K,V> extends OutputFormat<Tuple2<String,K>, V> {
    private AvroKeyOutputFormat<K> outputFormat;

    public MultipleAvroOutputsFormat() {
        this.outputFormat = new AvroKeyOutputFormat();
    }

    @Override
    public RecordWriter<Tuple2<String, K>, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordWriter<Tuple2<String, K>, V>() {
            private Job job = Job.getInstance(context.getConfiguration());

            private ReduceContextImpl ioContext = new ReduceContextImpl(job.getConfiguration(),context.getTaskAttemptID(),
                    new DummyIterator(), new GenericCounter(), new GenericCounter(),
                    new DummyRecordWriter(), new DummyOutputCommitter(), new DummyReporter(), null,
                    NullWritable.class, NullWritable.class);
            private AvroMultipleOutputs avroMultipleOutputs = new AvroMultipleOutputs(ioContext);

            @Override
            public void write(Tuple2<String, K> stringKTuple2, V v) throws IOException, InterruptedException {
                avroMultipleOutputs.write(stringKTuple2._1,stringKTuple2._2,v);
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            avroMultipleOutputs.close();
            }
        };
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        this.outputFormat.checkOutputSpecs(jobContext);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return outputFormat.getOutputCommitter(taskAttemptContext);
    }

    private class DummyOutputCommitter extends OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext) { }

        @Override
        public void setupTask(TaskAttemptContext taskAttemptContext) { }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
            return false;
        }

        @Override
        public void commitTask(TaskAttemptContext taskAttemptContext) { }

        @Override
        public void abortTask(TaskAttemptContext taskAttemptContext) { }
    }

    private class DummyRecordWriter extends RecordWriter<K, V> {
        @Override
        public void write(K k, V v) { }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) { }
    }

    private class DummyIterator implements RawKeyValueIterator {
        @Override
        public DataInputBuffer getKey()  {
            return null;
        }

        @Override
        public DataInputBuffer getValue() {
            return null;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public void close() { }

        @Override
        public Progress getProgress() {
            return null;
        }
    }
}
