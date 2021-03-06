/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of an asynchronous request from {@link ConsumerNetworkClient}. Use {@link ConsumerNetworkClient#poll(long)}
 * (and variants) to finish a request future. Use {@link #isDone()} to check if the future is complete, and
 * {@link #succeeded()} to check if the request completed successfully. Typical usage might look like this:
 *
 * <pre>
 *     RequestFuture<ClientResponse> future = client.send(api, request);
 *     client.poll(future);
 *
 *     if (future.succeeded()) {
 *         ClientResponse response = future.value();
 *         // Handle response
 *     } else {
 *         throw future.exception();
 *     }
 * </pre>
 *
 * @param <T> Return type of the result (Can be Void if there is no response)
 */
public class RequestFuture<T> {

    // 当前请求是否已完成
    private boolean isDone = false;
    // 记录请求正常完成时收到的响应
    private T value;
    // 记录导致请求异常的异常类，与value互斥
    private RuntimeException exception;
    /**
     * 用来监听请求完成的情况
     * RequestFutureListener有onSuccess()和onFailure()两个方法
     */
    private List<RequestFutureListener<T>> listeners = new ArrayList<>();


    /**
     * Check whether the response is ready to be handled
     * @return true if the response is ready, false otherwise
     */
    public boolean isDone() {
        return isDone;
    }

    /**
     * Get the value corresponding to this request (only available if the request succeeded)
     * @return the value if it exists or null
     */
    public T value() {
        return value;
    }

    /**
     * Check if the request succeeded;
     * @return true if the request completed and was successful
     */
    public boolean succeeded() {
        return isDone && exception == null;
    }

    /**
     * Check if the request failed.
     * @return true if the request completed with a failure
     */
    public boolean failed() {
        return isDone && exception != null;
    }

    /**
     * Check if the request is retriable (convenience method for checking if
     * the exception is an instance of {@link RetriableException}.
     * @return true if it is retriable, false otherwise
     */
    public boolean isRetriable() {
        return exception instanceof RetriableException;
    }

    /**
     * Get the exception from a failed result (only available if the request failed)
     * @return The exception if it exists or null
     */
    public RuntimeException exception() {
        return exception;
    }

    /**
     * Complete the request successfully. After this call, {@link #succeeded()} will return true
     * and the value can be obtained through {@link #value()}.
     * @param value corresponding value (or null if there is none)
     */
    public void complete(T value) {
        if (isDone)
            throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
        this.value = value;
        this.isDone = true;
        fireSuccess();
    }

    /**
     * Raise an exception. The request will be marked as failed, and the caller can either
     * handle the exception or throw it.
     * @param e corresponding exception to be passed to caller
     */
    public void raise(RuntimeException e) {
        if (isDone)
            throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
        this.exception = e;
        this.isDone = true;
        fireFailure();
    }

    /**
     * Raise an error. The request will be marked as failed.
     * @param error corresponding error to be passed to caller
     */
    public void raise(Errors error) {
        raise(error.exception());
    }

    private void fireSuccess() {
        for (RequestFutureListener<T> listener : listeners)
            listener.onSuccess(value);
    }

    private void fireFailure() {
        for (RequestFutureListener<T> listener : listeners)
            listener.onFailure(exception);
    }

    /**
     * Add a listener which will be notified when the future completes
     * @param listener
     */
    public void addListener(RequestFutureListener<T> listener) {
        // 当前请求是否已经完成
        if (isDone) {
            // 已完成
            if (exception != null)
                // 如果有异常，直接调用listener的onFailure()
                listener.onFailure(exception);
            else
                // 无异常，调用listener的onSuccess()
                listener.onSuccess(value);
        } else {
            this.listeners.add(listener);
        }
    }

    /**
     * Convert from a request future of one type to another type
     * @param adapter The adapter which does the conversion
     * @param <S> The type of the future adapted to
     * @return The new future
     *
     * 将RequestFuture<T>适配为RequestFuture<S>
     */
    public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
        // 适配结果
        final RequestFuture<S> adapted = new RequestFuture<S>();
        // 在当前的RequestFuture上添加监听器
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                adapter.onSuccess(value, adapted);
            }

            @Override
            public void onFailure(RuntimeException e) {
                adapter.onFailure(e, adapted);
            }
        });
        return adapted;
    }

    public void chain(final RequestFuture<T> future) {
        // 添加监听器
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                // 将value传递给下一个RequestFuture
                future.complete(value);
            }

            @Override
            public void onFailure(RuntimeException e) {
                // 将异常传递给下一个RequestFuture
                future.raise(e);
            }
        });
    }

    public static <T> RequestFuture<T> failure(RuntimeException e) {
        RequestFuture<T> future = new RequestFuture<T>();
        future.raise(e);
        return future;
    }

    public static RequestFuture<Void> voidSuccess() {
        RequestFuture<Void> future = new RequestFuture<Void>();
        future.complete(null);
        return future;
    }

    public static <T> RequestFuture<T> coordinatorNotAvailable() {
        return failure(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.exception());
    }

    public static <T> RequestFuture<T> leaderNotAvailable() {
        return failure(Errors.LEADER_NOT_AVAILABLE.exception());
    }

    public static <T> RequestFuture<T> noBrokersAvailable() {
        return failure(new NoAvailableBrokersException());
    }

    public static <T> RequestFuture<T> staleMetadata() {
        return failure(new StaleMetadataException());
    }

}
