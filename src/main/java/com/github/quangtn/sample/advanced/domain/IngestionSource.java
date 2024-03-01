package com.github.quangtn.sample.advanced.domain;

public class IngestionSource {

    public final Integer id;

    public final T input;

    public IngestionSource(Integer id, T input) {
        this.id = id;
        this.input = input;
    }
}
