package org.expand.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.Serializable;

public class ExpandRecordWriter<K, V> implements RecordWriter<K, V>, Serializable {
    private Path outputPath;
    private FSDataOutputStream out;

    public ExpandRecordWriter(Configuration conf, Path outputPath) throws IOException {
        this.out = outputPath.getFileSystem(conf).append(outputPath);
    }

    @Override
    public void write(K key, V value) throws IOException {
        System.out.println("ESTOY ENTRAND EN WRITE DE XPN");
	    // String towr = "{key: " + key.toString() + ", value: " + value.toString() + "}\n";
        String towr = key.toString() + value.toString();
	System.out.println("CADENA ESCRITA: " + towr);
        out.write(towr.getBytes());
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        out.close();
    }
}
