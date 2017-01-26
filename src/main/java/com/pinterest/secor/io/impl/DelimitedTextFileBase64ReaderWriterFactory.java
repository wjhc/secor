package com.pinterest.secor.io.impl;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.util.Base64;

public class DelimitedTextFileBase64ReaderWriterFactory extends DelimitedTextFileReaderWriterFactory {

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
        return new DelimitedTextFileBase64Writer(logFilePath, codec);
    }

    protected class DelimitedTextFileBase64Writer extends DelimitedTextFileWriter {
        private final Base64.Encoder encoder = Base64.getEncoder();

        public DelimitedTextFileBase64Writer(LogFilePath path, CompressionCodec codec) throws IOException {
            super(path, codec);
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            byte[] base64Value = encoder.encode(keyValue.getValue());
            super.write(new KeyValue(keyValue.getOffset(), keyValue.getKafkaKey(), base64Value));
        }
    }
}
