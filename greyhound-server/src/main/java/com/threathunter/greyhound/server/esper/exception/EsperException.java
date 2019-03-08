package com.threathunter.greyhound.server.esper.exception;

/**
 * This means the required operation is not supported.
 *
 * @author Wen Lu
 */
public class EsperException extends RuntimeException {
    public EsperException() {
        super();
    }

    public EsperException(String message) {
        super(message);
    }

    public EsperException(String message, Throwable cause) {
        super(message, cause);
    }
}
