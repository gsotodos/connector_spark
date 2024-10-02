package org.expand.spark;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class ExpandRecordReader<K, V> extends RecordReader<K, V> {

    private K key;
    private V value;
    private long start;
    private long end;
    private long pos;
    private FSDataInputStream fsin;
    private static final int bufsize = 8388608;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        pos = start;
        Path file = fileSplit.getPath();
	    fsin = file.getFileSystem(context.getConfiguration()).open(file);
        fsin.seek(start);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (pos < end) {
            key = (K) new Long(pos);
	        byte[] buffer = new byte[bufsize];
            int bytesRead = fsin.read(buffer);
            if (bytesRead > 0) {
                value = (V) new String(buffer, 0, bytesRead);
                pos += bytesRead;
                return true;
            }
        }
        return false;
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (fsin != null) {
            fsin.close();
        }
    }
}

