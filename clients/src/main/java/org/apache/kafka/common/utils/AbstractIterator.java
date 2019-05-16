/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A base class that simplifies implementing an iterator
 * @param <T> The type of thing we are iterating over
 */
public abstract class AbstractIterator<T> implements Iterator<T> {

    private static enum State {
        READY, // 迭代器已经准备好迭代下一项
        NOT_READY, // 迭代器未准备好迭代下一项，需要调用maybeComputeNext()
        DONE, // 当前迭代已经结束
        FAILED // 当前迭代器在迭代过程中出现异常
    };

    private State state = State.NOT_READY;
    private T next;

    @Override
    public boolean hasNext() {
        // 根据迭代器状态来决定做什么
        switch (state) {
            case FAILED:
                // 迭代过程出现异常，抛出IllegalStateException异常
                throw new IllegalStateException("Iterator is in failed state");
            case DONE:
                // 迭代已结束，返回false表示没有下一项了
                return false;
            case READY:
                // 已经准备好迭代下一项，返回true
                return true;
            default:
                // 否则调用maybeComputeNext()并返回
                return maybeComputeNext();
        }
    }

    // 返回下一个
    @Override
    public T next() {
        // 如果没有下一项，抛出NoSuchElementException异常
        if (!hasNext())
            throw new NoSuchElementException();
        // 设置状态
        state = State.NOT_READY;
        // 如果下一项为null，抛出IllegalStateException异常
        if (next == null)
            throw new IllegalStateException("Expected item but none found.");
        // 否则返回next
        return next;
    }

    // 没有移出操作
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }

    /**
     * 查看下一个
     * 与next()不同的是，这里不会设置state为NOT_READY，
     * 即在下一次调用hasNext()时不会执行maybeComputeNext()
     */
    public T peek() {
        // 如果没有下一项，抛出NoSuchElementException异常
        if (!hasNext())
            throw new NoSuchElementException();
        // 否则返回next
        return next;
    }

    // 完成了迭代
    protected T allDone() {
        // 设置状态
        state = State.DONE;
        return null;
    }

    protected abstract T makeNext();

    private Boolean maybeComputeNext() {
        // 先设置state为FAILED
        state = State.FAILED;
        // 用next记录下一项
        next = makeNext();
        if (state == State.DONE) {
            // 如果state为NONE，返回false
            return false;
        } else {
            // 否则设置state为READY，返回true
            state = State.READY;
            return true;
        }
    }

}
