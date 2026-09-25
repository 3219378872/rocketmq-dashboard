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
import java.nio.file.Paths;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.test.base.IntegrationTestBase;

public class DlqBrokerFixture {
    public static void main(String[] args) {
        int status = 0;
        try {
            System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
            NamesrvController ns = IntegrationTestBase.createAndStartNamesrv();
            String nsAddr = "127.0.0.1:" + ns.getNettyServerConfig().getListenPort();
            BrokerController broker = IntegrationTestBase.createAndStartBroker(nsAddr);
            String group = "boundary-" + java.util.UUID.randomUUID().toString();
            broker.getTopicConfigManager().updateTopicConfig(new org.apache.rocketmq.common.TopicConfig("%DLQ%" + group, 1, 1));
            broker.registerBrokerAll(true, false, true);
            org.apache.rocketmq.test.util.MQAdminTestUtils.startAdmin(nsAddr);
            Files.write(Paths.get(args[0]), (nsAddr + "\n" + broker.getBrokerAddr() + "\n"
                    + broker.getBrokerConfig().getBrokerName() + "\n" + group + "\n").getBytes(StandardCharsets.UTF_8));
            long deadline = System.currentTimeMillis() + 120_000;
            while (!Files.exists(Paths.get(args[1])) && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
        } catch (Throwable error) {
            error.printStackTrace();
            status = 1;
        }
        // IntegrationTestBase's shutdown hook closes brokers/nameservers and destroys stores.
        System.exit(status);
    }
}
