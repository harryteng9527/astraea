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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.collector.MetricCollector;

public class NetworkIngressAssignor extends Assignor {

  @Override
  protected Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.assignor.Subscription> subscriptions,
      Set<TopicPartition> topicPartitions) {
    var assignments = new HashMap<String, List<TopicPartition>>();
    var topics = new HashSet<String>();
    Map<TopicPartition, Double> partitionCost;
    ClusterBean clusterBean;
    var consumers = new HashSet<>(subscriptions.keySet());
    for (var subscription : subscriptions.entrySet()) {
      assignments.put(subscription.getKey(), new ArrayList<>());
      topics.addAll(subscription.getValue().topics());
    }
    try (var metricCollector = MetricCollector.builder().build()) {
      metricCollector.registerJmx(
          1001, InetSocketAddress.createUnresolved("192.168.103.171", 8000));
      metricCollector.registerJmx(
          1002, InetSocketAddress.createUnresolved("192.168.103.172", 8000));
      metricCollector.registerJmx(
          1003, InetSocketAddress.createUnresolved("192.168.103.173", 8000));
      costFunction.fetcher().ifPresent(metricCollector::addFetcher);
      Utils.sleep(Duration.ofSeconds(2));
      clusterBean = metricCollector.clusterBean();
    }

    try (var admin = Admin.of("192.168.103.171:9092,192.168.103.172:9092,192.168.103.173:9092")) {
      partitionCost =
          costFunction
              .partitionCost(admin.clusterInfo(topics).toCompletableFuture().join(), clusterBean)
              .value();
    }

    return assignByCost(partitionCost, consumers);
  }

  @Override
  public String name() {
    return "networkIngress";
  }

  Map<String, List<TopicPartition>> roundrobinAssign(
      Set<TopicPartition> subscribedPartitions, Set<String> consumers) {
    var firstAssignment = new HashMap<String, List<TopicPartition>>();
    var partitions =
        subscribedPartitions.stream().sorted().collect(Collectors.toUnmodifiableList());

    var consumerList = consumers.stream().sorted().collect(Collectors.toUnmodifiableList());
    var numberOfConsumer = consumers.size();

    consumerList.forEach(c -> firstAssignment.put(c, new ArrayList<>()));

    IntStream.range(0, partitions.size())
        .forEach(
            i -> {
              var index = i % numberOfConsumer;
              firstAssignment.get(consumerList.get(index)).add(partitions.get(i));
            });
    return firstAssignment;
  }

  Map<String, List<TopicPartition>> assignByCost(
      Map<TopicPartition, Double> partitionCost, Set<String> consumers) {
    var costPerConsumer = new HashMap<String, Double>();
    var assignment = new HashMap<String, List<TopicPartition>>();
    var sortedPartitionCost = new LinkedHashMap<TopicPartition, Double>();
    // sort partition cost by value
    partitionCost.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .forEachOrdered(x -> sortedPartitionCost.put(x.getKey(), x.getValue()));

    // initial
    consumers.forEach(
        c -> {
          costPerConsumer.put(c, (double) 0);
          assignment.put(c, new ArrayList<>());
        });

    sortedPartitionCost.forEach(
        (tp, cost) -> {
          // find the consumer that cost is lowest
          var consumerWithLowestCost =
              Collections.min(costPerConsumer.entrySet(), Map.Entry.comparingByValue());
          var consumer = consumerWithLowestCost.getKey();

          // add cost value
          costPerConsumer.computeIfPresent(consumer, (ignore, cos) -> cos + cost);
          assignment.get(consumer).add(tp);
        });
    assignment.forEach(
        (consumer, assign) ->
            System.out.println("consumer #" + consumer + ", its assignment =" + assign));
    return assignment;
  }
}
