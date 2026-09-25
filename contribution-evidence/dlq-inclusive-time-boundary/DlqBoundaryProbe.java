/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.studio.cluster.broker.MqClientPool;
import org.apache.rocketmq.studio.cluster.broker.RuntimeAdminClientResolver;
import org.apache.rocketmq.studio.instance.dlq.DLQExportResultVO;
import org.apache.rocketmq.studio.provider.apache.RocketMQDLQProvider;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class DlqBoundaryProbe {
    public static void main(String[] args) throws Exception {
        List<String> ready = Files.readAllLines(Path.of(args[0]));
        String id = UUID.randomUUID().toString();
        String group = ready.get(3);
        String topic = "%DLQ%" + group;
        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr(ready.get(0));
        admin.setInstanceName("admin-" + id);
        DefaultMQProducer producer = new DefaultMQProducer("producer-" + id);
        producer.setNamesrvAddr(ready.get(0));
        producer.setInstanceName("producer-" + id);
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("reader-" + id);
        consumer.setNamesrvAddr(ready.get(0));
        consumer.setInstanceName("reader-" + id);
        try {
            admin.start();
            producer.start();
            consumer.start();
            MessageQueue queue = new MessageQueue(topic, ready.get(2), 0);
            List<Message> batch = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                batch.add(new Message(topic, ("dead-letter-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            producer.send(batch);
            long deadline = System.currentTimeMillis() + 15_000;
            while (consumer.maxOffset(queue) < 80 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            PullResult truth = consumer.pull(queue, "*", 0, 128);
            List<MessageExt> messages = truth.getMsgFoundList();
            if (messages == null || messages.size() != 80) {
                throw new AssertionError("Fixture did not expose all 80 batch messages: " + truth);
            }
            long timestamp = messages.getFirst().getStoreTimestamp();
            if (messages.stream().anyMatch(message -> message.getStoreTimestamp() != timestamp)) {
                throw new AssertionError("Batch did not share one broker-assigned store timestamp");
            }
            while (System.currentTimeMillis() <= timestamp + 5) {
                Thread.sleep(1);
            }
            producer.send(new Message(topic, "outside-window".getBytes(StandardCharsets.UTF_8)));
            RuntimeAdminClientResolver resolver = new RuntimeAdminClientResolver(null, null, null, null) {
                @Override
                public <T> T executePullConsumer(String instance, MqClientPool.ClientAction<DefaultMQPullConsumer, T> action) {
                    try {
                        return action.apply(consumer);
                    } catch (Exception error) {
                        throw new IllegalStateException(error);
                    }
                }
            };
            RocketMQDLQProvider provider = new RocketMQDLQProvider(resolver, null);
            DLQExportResultVO result = provider.exportMessages("isolated-fixture", group, timestamp - 1, timestamp, 1000);
            String record = "LIVE_DLQ expected=80 actual=" + result.getMessages().size()
                    + " failedQueues=" + result.getFailedQueueCount() + " truncated=" + result.isTruncated()
                    + " timestamp=" + timestamp + " lowerEnd=" + consumer.searchOffset(queue, timestamp)
                    + " exclusiveEnd=" + consumer.searchOffset(queue, timestamp + 1);
            System.out.println(record);
            Files.writeString(Path.of(args[1]), record + "\n");
            if (result.getMessages().stream().anyMatch(message -> message.getStoreTime() > timestamp)) {
                throw new AssertionError("Message outside the time window was exported");
            }
            if (result.getMessages().size() != Integer.parseInt(args[2])) {
                throw new AssertionError(record);
            }
        } finally {
            consumer.shutdown();
            producer.shutdown();
            admin.shutdown();
        }
    }
}
