package com.example.likecounter.exception;

public class InvalidVideoIdException extends RuntimeException {

    public InvalidVideoIdException(String message) {
        super(message);
    }
}
