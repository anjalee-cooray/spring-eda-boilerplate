package com.example.eda.gateway.idempotency;

/**
 * Value object stored in Redis for idempotency caching.
 * Serialized as "statusCode|body" — pipe delimiter chosen because
 * JSON bodies never contain a raw pipe at the top level.
 */
record IdempotencyCachedResponse(int statusCode, String body) {

    private static final String DELIMITER = "|";

    String serialize() {
        return statusCode + DELIMITER + (body != null ? body : "");
    }

    static IdempotencyCachedResponse deserialize(String value) {
        int delimiterIndex = value.indexOf(DELIMITER);
        if (delimiterIndex < 0) {
            return new IdempotencyCachedResponse(200, value);
        }
        int status = Integer.parseInt(value.substring(0, delimiterIndex));
        String body = value.substring(delimiterIndex + 1);
        return new IdempotencyCachedResponse(status, body);
    }
}
