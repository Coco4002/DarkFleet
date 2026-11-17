package com.darkfleet.intelligence.web;

import org.springframework.http.HttpStatus;

/** A request the API refuses, with the status and a message saying what to change. */
public class ApiException extends RuntimeException {

    private final HttpStatus status;

    private ApiException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus status() {
        return status;
    }

    /** 400: malformed input (a bad parameter, a time window that is too long). */
    public static ApiException badRequest(String message) {
        return new ApiException(HttpStatus.BAD_REQUEST, message);
    }

    public static ApiException notFound(String message) {
        return new ApiException(HttpStatus.NOT_FOUND, message);
    }

    /** 409: valid, but the current state doesn't allow it (e.g. the proposal was already decided). */
    public static ApiException conflict(String message) {
        return new ApiException(HttpStatus.CONFLICT, message);
    }

    /** 503: temporarily unable to serve; the client should retry later. */
    public static ApiException unavailable(String message) {
        return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, message);
    }

    /** 422: well-formed, but its content breaks a rule (e.g. a proposal for an unknown detection). */
    public static ApiException unprocessable(String message) {
        return new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, message);
    }
}
