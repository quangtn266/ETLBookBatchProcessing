package com.github.quangtn.sample.basic.sinks;

import com.github.quangtn.sample.basic.domain.Book;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

public class BookJdbcSink implements Function<DataStream<Book>, DataStreamSink<Book>> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final JdbcExecutionOptions executionOptions;

    private final JdbcConnectionOptions connectionOptions;

    public BookJdbcSink(
            JdbcExecutionOptions executionOptions,
            JdbcConnectionOptions connectionOptions) {
        this.executionOptions = executionOptions;
        this.connectionOptions = connectionOptions;
    }

    public static void accept(PreparedStatement s, Book b) throws SQLException {
        s.setString(1, b.asin);
        s.setString(2, b.isbn);

    }
}
