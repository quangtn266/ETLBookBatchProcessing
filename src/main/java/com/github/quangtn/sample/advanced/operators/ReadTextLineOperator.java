package com.github.quangtn.sample.advanced.operators;

import com.github.quangtn.sample.advanced.domain.IngestionSource;
import com.github.quangtn.sample.shared.fs.TextLineFormat;
import java.util.function.Function;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;

public class ReadTextLineOperator implements Function<DataStream<IngestionSource<Path>>,
        SingleOutputStreamOperator<IngestionSource<String>>>, FlatMapFunction<IngestionSource<Path>,
        IngestionSource<String>> {

    private static final TextLineFormat FORMAT = new TextLineFormat();

    @Override
    public SingleOutputStreamOperator<IngestionSource<String>> apply (DataStream<IngestionSource<Path>> in) {
        return in.flatMap(this).name("read text lines");
    }

    @Override
    public void flatMap(IngestionSource<Path> ingestionSource, Collector<IngestionSource<String>> out) throws Exception {
        try (BufferedReader reader = FORMAT.createReader(ingestionSource.input)) {
            Integer id = ingestionSource.id;
            String line;
            while ((line = reader.readLine()) != null) {
                out.collect(new IngestionSource<>(id, line));
            }
        }
    }
}
