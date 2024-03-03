package com.github.quangtn.sample.advanced.streams;

import com.github.quangtn.sample.basic.operators.BookValidatorOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.github.quangtn.sample.advanced.domain.IngestionSource;
import com.github.quangtn.sample.advanced.operators.AddIngestionLogEntryJdbcOperator;
import com.github.quangtn.sample.advanced.operators.BookJsonDeserializerOperator;
import com.github.quangtn.sample.advanced.operators.ReadTextLineOperator;
import com.github.quangtn.sample.advanced.sinks.BookIngestionJdbcSink;
import com.github.quangtn.sample.advanced.sources.BookDataStreamSource;
import com.github.quangtn.sample.basic.domain.Book;


import java.util.Optional;
import java.util.function.Function;
import java.util.function.Consumer;

public class BooksIngestionStream implements Consumer<StreamExecutionEnvironment>,
        Function<DataStreamSource<Path>, SingleOutputStreamOperator<IngestionSource<Book>>> {

    private final Options options;

    BooksIngestionStream(Options options) { this.options = options; }

    public static BooksIngestionStream getStream(Options options) { return new BooksIngestionStream(options); }

    @Override
    public void accept(StreamExecutionEnvironment env) {
        new BookDataStreamSource(options.inputDir).andThen(this).andThen(new BookIngestionJdbcSink(
                options.jdbc.execution,
                options.jdbc.connection)).apply(env);
    }

    @Override
    public SingleOutputStreamOperator<IngestionSource<Book>> apply(DataStreamSource<Path> source) {
        return new AddIngestionLogEntryJdbcOperator(options.jdbc.connection)
                .andThen(new ReadTextLineOperator())
                .andThen(new BookJsonDeserializerOperator())
                .apply(source);
    }


    public static class Options {
        public final Path inputDir;
        public final Jdbc jdbc;

        Options(ParameterTool params) {
            inputDir = new Path(Optional.ofNullable(params.get("input-dir")).orElse("./"));
            jdbc = new Jdbc(params);
        }

        public static Options fromArgs(String[] args) { return new Options(ParameterTool.fromArgs(args)); }

        public static class Jdbc {

            public final JdbcConnectionOptions connection;

            public final JdbcExecutionOptions execution;

            Jdbc(ParameterTool params) {
                connection = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(
                                Optional.ofNullable(params.get("db-url")).orElse(
                                        "jdbc:postgresql://localhost:5432/books?user=postgres"
                                )
                        ).withDriverName("org.postgresql.Driver").build();

                execution = JdbcExecutionOptions.builder().withBatchSize(100).withBatchIntervalMs(200)
                        .withMaxRetries(5).build();
            }
        }
    }
}
