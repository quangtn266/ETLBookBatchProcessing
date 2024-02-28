package com.github.quangtn.sample.basic.streams;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import com.github.quangtn.sample.basic.domain.ErrorState;
import com.github.quangtn.sample.basic.sinks.ErrorStateJdbcSink;
import org.apache.flink.util.OutputTag;



public class ErrorStateSideOutputDataStream<T> implements
        UnaryOperator<SingleOutputStreamOperator<T>>,
        Consumer<SideOutputDataStream<ErrorState>> {

    private static final OutputTag<ErrorState> OUTPUT_TAG = new OutputTag<ErrorState>("validation-errors") {};

    private final JdbcExecutionOptions executionOptions;

    private final JdbcConnectionOptions connectionOptions;

    public ErrorStateSideOutputDataStream(
            JdbcExecutionOptions executionOptions,
            JdbcConnectionOptions connectionOptions
    ) {
        this.executionOptions = executionOptions;
        this.connectionOptions = connectionOptions;
    }

    @Override
    public SingleOutputStreamOperator<T> apply(SingleOutputStreamOperator<T> in) {
        accept(in.getSideOutput(OUTPUT_TAG));
        return in;
    }

    @Override
    public void accept(SideOutputDataStream<ErrorState> stream) {
        new ErrorStateJdbcSink(executionOptions, connectionOptions).apply(stream);
    }
}
