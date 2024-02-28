package com.github.quangtn.sample.basic;

import com.github.quangtn.sample.basic.streams.BooksIngestionStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class BasicBookIngestion {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getBoolean("disable-operator-chaining", false)) {
            env.disableOperatorChaining();
        }

        BooksIngestionStream.getStream(BooksIngestionStream.Options.fromArgs(args)).accept(env);

        env.execute("Sample book basic ETL job");
    }
}
