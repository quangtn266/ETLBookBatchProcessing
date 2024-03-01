package com.github.quangtn.sample.advanced;

import org.apache.flink.api.common.JobExecutionResult;
import com.github.quangtn.sample.basic.streams.BooksIngestionStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AdvancedBooksIngestion {

    private static final Logger LOGGER = LogManager.getLogger();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BooksIngestionStream.getStream(BooksIngestionStream.Options.fromArgs(args)).accept(env);

        LOGGER.info("execute");
        JobExecutionResult result = env.execute("Sample books Advanced ETL job");
        LOGGER.info("done, {} ms", result.getNetRuntime());
    }
}
