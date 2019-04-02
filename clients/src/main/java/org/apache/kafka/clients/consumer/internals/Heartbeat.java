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

/**
 * A helper class for managing the heartbeat to the coordinator
 */
public final class Heartbeat {
    // 过期时间
    private final long timeout;
    // 两次发送心跳的间隔
    private final long interval;
    // 最后发送心跳的时间
    private long lastHeartbeatSend;
    // 最后收到心跳响应的时间
    private long lastHeartbeatReceive;
    // 心跳任务重置时间
    private long lastSessionReset;

    public Heartbeat(long timeout,
                     long interval,
                     long now) {
        if (interval >= timeout)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");

        this.timeout = timeout;
        this.interval = interval;
        this.lastSessionReset = now;
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
    }

    public void receiveHeartbeat(long now) {
        this.lastHeartbeatReceive = now;
    }

    public boolean shouldHeartbeat(long now) {
        return timeToNextHeartbeat(now) == 0;
    }
    
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    // 计算下次发送心跳的时间
    public long timeToNextHeartbeat(long now) {
        long timeSinceLastHeartbeat = now - Math.max(lastHeartbeatSend, lastSessionReset);

        if (timeSinceLastHeartbeat > interval)
            return 0;
        else
            return interval - timeSinceLastHeartbeat;
    }

    // 检测是否过期
    public boolean sessionTimeoutExpired(long now) {
        return now - Math.max(lastSessionReset, lastHeartbeatReceive) > timeout;
    }

    public long interval() {
        return interval;
    }

    public void resetSessionTimeout(long now) {
        this.lastSessionReset = now;
    }

}