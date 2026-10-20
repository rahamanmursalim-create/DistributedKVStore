package com.distributedkv.common;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A generic result container for operation results
 *
 * @param <T> the type of the data contained in the result
 */
public class Result<T> {
    private final Status status;
    private final String message;
    private final T data;

    /**
     * Private constructor to enforce factory methods
     *
     * @param status the operation status
     * @param message detailed message (may be null)
     * @param data result data (may be null)
     */
    private Result(Status status, String message, T data) {
        this.status = status != null ? status : Status.UNKNOWN_ERROR;
        this.message = message;
        this.data = data;
    }

    /**
     * Create a successful result with data
     *
     * @param <T> the type of the data
     * @param data the result data
     * @return a successful result with the provided data
     */
    public static <T> Result<T> success(T data) {
        return new Result<>(Status.OK, null, data);
    }

    /**
     * Create a successful result with no data
     *
     * @param <T> the type of the data
     * @return a successful result with null data
     */
    public static <T> Result<T> success() {
        return new Result<>(Status.OK, null, null);
    }

    /**
     * Create a failed result with a status and message
     *
     * @param <T> the type of the data
     * @param status the error status
     * @param message detailed error message
     * @return a failed result
     */
    public static <T> Result<T> fail(Status status, String message) {
        return new Result<>(status, message, null);
    }

    /**
     * Create a failed result with just a status
     *
     * @param <T> the type of the data
     * @param status the error status
     * @return a failed result
     */
    public static <T> Result<T> fail(Status status) {
        return new Result<>(status, status.getMessage(), null);
    }

    /**
     * Check if the result is successful
     *
     * @return true if status is OK
     */
    public boolean isSuccess() {
        return status.isOk();
    }

    /**
     * Get the status of the operation
     *
     * @return the status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Get the detailed message
     *
     * @return the message, may be null
     */
    public String getMessage() {
        return message != null ? message : status.getMessage();
    }

    /**
     * Get the result data
     *
     * @return the data, may be null
     */
    public T getData() {
        return data;
    }

    /**
     * Get the result data as an Optional
     *
     * @return an Optional containing the data, or empty if null
     */
    public Optional<T> getOptionalData() {
        return Optional.ofNullable(data);
    }

    /**
     * Convert this Result to a CompletableFuture
     *
     * @return a completed future with this result
     */
    public CompletableFuture<Result<T>> toFuture() {
        return CompletableFuture.completedFuture(this);
    }

    /**
     * Map the data of this result to another type
     *
     * @param <R> the target type
     * @param mapper the function to map the data
     * @return a new result with the mapped data, or the original error
     */
    public <R> Result<R> map(java.util.function.Function<T, R> mapper) {
        if (isSuccess() && data != null) {
            try {
                return Result.success(mapper.apply(data));
            } catch (Exception e) {
                return Result.fail(Status.INTERNAL_ERROR, "Exception during mapping: " + e.getMessage());
            }
        }
        return Result.fail(status, message);
    }

    /**
     * Flat map this result to another result
     *
     * @param <R> the target type
     * @param mapper the function to map the data to another result
     * @return the mapped result, or the original error
     */
    public <R> Result<R> flatMap(java.util.function.Function<T, Result<R>> mapper) {
        if (isSuccess() && data != null) {
            try {
                return mapper.apply(data);
            } catch (Exception e) {
                return Result.fail(Status.INTERNAL_ERROR, "Exception during flat mapping: " + e.getMessage());
            }
        }
        return Result.fail(status, message);
    }

    @Override
    public String toString() {
        if (isSuccess()) {
            return "Success[data=" + data + "]";
        } else {
            return "Failure[status=" + status + ", message=" + getMessage() + "]";
        }
    }
}