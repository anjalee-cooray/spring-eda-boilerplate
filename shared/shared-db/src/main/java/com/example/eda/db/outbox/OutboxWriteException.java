package com.example.eda.db.outbox;

public class OutboxWriteException extends RuntimeException {

    public OutboxWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
