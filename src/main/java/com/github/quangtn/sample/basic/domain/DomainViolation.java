package com.github.quangtn.sample.basic.domain;

public class DomainViolation {

    public final String location;

    public final String message;

    public DomainViolation(String location, String message) {
        this.location = location;
        this.message = message;
    }
}
