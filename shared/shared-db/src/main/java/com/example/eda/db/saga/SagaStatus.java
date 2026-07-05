package com.example.eda.db.saga;

public enum SagaStatus {

    /** First step has been sent; awaiting first response event. */
    STARTED,

    /** Mid-saga: at least one step completed, awaiting next response. */
    RUNNING,

    /** All steps completed successfully. */
    COMPLETED,

    /** A step failed; compensation steps are being executed in reverse. */
    COMPENSATING,

    /** All compensation steps completed. */
    COMPENSATED,

    /** Saga ended in an unrecoverable error (compensation also failed). */
    FAILED
}
