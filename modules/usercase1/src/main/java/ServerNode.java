/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ServerNode {

    private static Logger log = LoggerFactory.getLogger(ServerNode.class);
    private static final String CACHE_NAME = "query";

    public static void main(String[] args) throws InterruptedException {
        /*try (*/Ignite ignite = Ignition.start("example-ignite.xml");/*)*/ {
            log.info("Server Start with id {}!!!", ignite.cluster().localNode().id());

            Thread.sleep(3000);

//            ignite.close();


        } /*finally {
            log.info("Server Stop!!!");
        }*/
    }

}
