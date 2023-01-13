/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.assignor;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NetworkIngressAssignorTest {
  @Test
  void testAssignorByCost() {
    var assignor = new NetworkIngressAssignor();
    var partitionCost =
        Map.of(
            TopicPartition.of("a-0"),
            0.5,
            TopicPartition.of("a-1"),
            0.1,
            TopicPartition.of("a-2"),
            0.3,
            TopicPartition.of("a-3"),
            0.5,
            TopicPartition.of("a-4"),
            0.3,
            TopicPartition.of("a-5"),
            0.5,
            TopicPartition.of("a-6"),
            0.1,
            TopicPartition.of("a-7"),
            0.1,
            TopicPartition.of("a-8"),
            0.3);

    var consumers = Set.of("c1", "c2", "c3");
    var assignments = assignor.assignByCost(partitionCost, consumers);

    var consumerPerCost =
        assignments.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> 0.0));

    for (var assignment : assignments.entrySet()) {
      var consumer = assignment.getKey();
      assignment
          .getValue()
          .forEach(
              tp -> {
                System.out.println("select tp #" + tp);
                consumerPerCost.put(
                    consumer, consumerPerCost.get(consumer) + partitionCost.get(tp));
              });
    }
    consumerPerCost.forEach(
        (consumer, cos) -> System.out.println("consumer #" + consumer + ", its cost =" + cos));
    assignments.forEach(
        (consumer, assignment) ->
            System.out.println("consumer #" + consumer + ", its assignment =" + assignment));
    var c1 = consumerPerCost.get("c1");
    var c2 = consumerPerCost.get("c2");
    var c3 = consumerPerCost.get("c3");
    Assertions.assertTrue(Objects.equals(c1, c2) && Objects.equals(c2, c3));
  }
}
