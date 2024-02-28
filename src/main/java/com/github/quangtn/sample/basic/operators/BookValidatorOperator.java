package com.github.quangtn.sample.basic.operators;

import java.util.List;
import java.util.function.Function;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.github.quangtn.sample.basic.domain.Book;
import com.github.quangtn.sample.basic.domain.BookValidator;
import com.github.quangtn.sample.basic.domain.DomainViolation;
import com.github.quangtn.sample.basic.domain.ErrorState;

public final class BookValidatorOperator extends ProcessFunction<Book, Book> implements
        Function<SingleOutputStreamOperator<Book>, SingleOutputStreamOperator<Book>> {

    private static final OutputTag<ErrorState> OUTPUT_TAG = new OutputTag<ErrorState>("validation-errors"){};

    @Override
    public SingleOutputStreamOperator<Book> apply(SingleOutputStreamOperator<Book> bookDataStream) {
        return bookDataStream.process(this).name("validate book");
    }

    @Override
    public void processElement(Book book, ProcessFunction<Book, Book>.Context context, Collector<Book> collector) {
            // asin is required to be able to uniquely track the record
        if(book.asin == null || book.asin.isEmpty()) {
            return;
        }

        List<DomainViolation> violations = BookValidator.validate(book);
        if(!violations.isEmpty()) {
            ErrorState errorState = new ErrorState();
            errorState.domain = "book";
            errorState.reference = book.asin;
            errorState.violations = violations;
            context.output(OUTPUT_TAG, errorState);
            return;
        }

        collector.collect(book);
    }
}
