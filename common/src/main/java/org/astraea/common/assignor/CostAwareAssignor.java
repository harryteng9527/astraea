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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.NoSufficientMetricsException;

/**
 * This assignor scores the partitions by cost function(s) that user given. Each cost function
 * evaluate the partitions' cost in each node by metrics depend on which cost function user use. The
 * default cost function ranks partitions that are in the same node by NetworkIngressCost{@link
 * org.astraea.common.cost.NetworkIngressCost}
 *
 * <p>When get the partitions' cost of each node, assignor would assign partitions to consumers base
 * on node. Each consumer would get the partitions with "the similar cost" from same node.
 *
 * <p>The important configs are JMX port, MAX_WAIT_BEAN, MAX_TRAFFIC_MiB_INTERVAL. Most cost
 * function need the JMX metrics to score partitions. Normally, all brokers use the same JMX port,
 * so you could just define the `jmx.port=12345`. If one of brokers uses different JMX client port,
 * you can define `broker.1001.jmx.port=3456` (`1001` is the broker id) to replace the value of
 * `jmx.port`. If the jmx port is undefined, only local mbean client is created for each cost
 * function.
 *
 * <p>MAX_WAIT_BEAN is the config of setting the amount of time waiting for fetch ClusterBean.
 * MAX_TRAFFIC_MiB_INTERVAL is the config of setting how traffic similar is. You can define these
 * config by `max.wait.bean=10` or `max.traffic.mib.interval=15`
 */
public class CostAwareAssignor extends Assignor {
  protected static final String MAX_RETRY_TIME = "max.retry.time";
  Duration maxRetryTime = Duration.ofSeconds(30);

  @Override
  protected Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.assignor.Subscription> subscriptions,
      ClusterInfo clusterInfo) {
    // TODO: Detect Unregister node and register them if any
    var subscribedTopics =
        subscriptions.values().stream()
            .map(org.astraea.common.assignor.Subscription::topics)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());

    // wait for clusterBean
    retry(clusterInfo);

    var clusterBean = metricStore.clusterBean();
    var partitionCost = costFunction.partitionCost(clusterInfo, clusterBean);
    var cost =
        partitionCost.value().entrySet().stream()
            .filter(e -> subscribedTopics.contains(e.getKey().topic()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    var incompatiblePartition = partitionCost.incompatibility();

    return greedyAssign(subscriptions, cost, incompatiblePartition);
  }

  protected Map<String, List<TopicPartition>> greedyAssign(
      Map<String, org.astraea.common.assignor.Subscription> subscriptions,
      Map<TopicPartition, Double> costs,
      Map<TopicPartition, Set<TopicPartition>> incompatiblePartition) {
    var tmpConsumerCost =
        subscriptions.keySet().stream()
            .map(c -> Map.entry(c, 0.0))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var tmpAssignment =
        subscriptions.keySet().stream()
            .map(v -> Map.entry(v, new ArrayList<TopicPartition>()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var lowestCostConsumer =
        (Function<TopicPartition, String>)
            (tp) -> {
              var subscribeConsumers =
                  subscriptions.entrySet().stream()
                      .filter(e -> e.getValue().topics().contains(tp.topic()))
                      .map(Map.Entry::getKey)
                      .collect(Collectors.toUnmodifiableSet());
              var suitableConsumers =
                  incompatiblePartition.isEmpty()
                      ? subscribeConsumers
                      : incompatiblePartition.get(tp).isEmpty()
                          ? subscribeConsumers
                          : subscribeConsumers.stream()
                              .filter(
                                  c ->
                                      tmpAssignment.get(c).stream()
                                          .noneMatch(
                                              p -> incompatiblePartition.get(tp).contains(p)))
                              .collect(Collectors.toUnmodifiableSet());

              return suitableConsumers.isEmpty()
                  ? tmpConsumerCost.entrySet().stream()
                      .filter(e -> subscribeConsumers.contains(e.getKey()))
                      .min(Map.Entry.comparingByValue())
                      .get()
                      .getKey()
                  : tmpConsumerCost.entrySet().stream()
                      .filter(e -> suitableConsumers.contains(e.getKey()))
                      .min(Map.Entry.comparingByValue())
                      .get()
                      .getKey();
            };

    return costs.entrySet().stream()
        .flatMap(
            e -> {
              var tp = e.getKey();
              var cost = e.getValue();
              var consumer = lowestCostConsumer.apply(tp);
              tmpConsumerCost.compute(consumer, (ignore, totalCost) -> cost + totalCost);
              tmpAssignment.get(consumer).add(tp);
              return Stream.of(Map.entry(consumer, tp));
            })
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> Collections.singletonList(entry.getValue()),
                (l1, l2) ->
                    Stream.of(l1, l2).flatMap(Collection::stream).collect(Collectors.toList())));
  }

  private void retry(ClusterInfo clusterInfo) {
    var timeoutMs = System.currentTimeMillis() + maxRetryTime.toMillis();
    while (System.currentTimeMillis() < timeoutMs) {
      try {
        var clusterBean = metricStore.clusterBean();
        var partitionCost = costFunction.partitionCost(clusterInfo, clusterBean);
        if (partitionCost.value().values().stream().noneMatch(v -> Double.isNaN(v))) return;
      } catch (NoSufficientMetricsException e) {
        e.printStackTrace();
        Utils.sleep(Duration.ofSeconds(1));
      }
    }
    throw new RuntimeException("Failed to fetch clusterBean due to timeout");
  }

  @Override
  protected void configure(Configuration config) {
    config.duration(MAX_RETRY_TIME).ifPresent(v -> this.maxRetryTime = v);
  }

  @Override
  public String name() {
    return "networkIngress";
  }
}