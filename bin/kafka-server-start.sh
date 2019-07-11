#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ $# -lt 1 ]; # 检查参数个数
then # 打印使用方法
	echo "USAGE: $0 [-daemon] server.properties [--override property=value]*"
	exit 1
fi

# 获取当前脚本所在的路径
base_dir=$(dirname $0)

# 设置Log4j相关的环境变量
if [ "x$KAFKA_LOG4J_OPTS" = "x" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/log4j.properties"
fi

# 设置JVM内存
if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
fi

EXTRA_ARGS="-name kafkaServer -loggc"

# 检测第一个参数是否为"-daemon"
COMMAND=$1
case $COMMAND in
  -daemon)
    EXTRA_ARGS="-daemon "$EXTRA_ARGS
    # 左移参数列表，即删除"-daemon"参数
    shift
    ;;
  *)
    ;;
esac

# 调用kafka-run-class.sh脚本，传入参数以及主类
exec $base_dir/kafka-run-class.sh $EXTRA_ARGS kafka.Kafka "$@"
