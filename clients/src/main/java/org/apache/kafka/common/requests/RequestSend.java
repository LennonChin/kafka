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
package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.protocol.types.Struct;

/**
 * A send object for a kafka request
 */
public class RequestSend extends NetworkSend {

    // 请求头
    private final RequestHeader header;
    // 请求体
    private final Struct body;

    public RequestSend(String destination, RequestHeader header, Struct body) {
        super(destination, serialize(header, body));
        this.header = header;
        this.body = body;
    }

    public static ByteBuffer serialize(RequestHeader header, Struct body) {
    	// 分配缓冲区，大小是header和body的总大小
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + body.sizeOf());
        // 写入请求头
        header.writeTo(buffer);
        // 写入请求体
        body.writeTo(buffer);
        //
        buffer.rewind();
        return buffer;
    }

    public RequestHeader header() {
        return this.header;
    }

    public Struct body() {
        return body;
    }

    @Override
    public String toString() {
        return "RequestSend(header=" + header.toString() + ", body=" + body.toString() + ")";
    }

}
