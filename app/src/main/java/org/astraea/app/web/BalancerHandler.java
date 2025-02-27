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
package org.astraea.app.web;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.BalancerConsole;
import org.astraea.common.balancer.BalancerProblemFormat;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.MigrationCost;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.collector.MetricStore;

class BalancerHandler implements Handler, AutoCloseable {

  private final Admin admin;
  private final BalancerConsole balancerConsole;
  private final Map<String, PostRequestWrapper> taskMetadata = new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<Balancer.Plan>> planGenerations =
      new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<Void>> planExecutions = new ConcurrentHashMap<>();

  private final MetricStore metricStore;

  BalancerHandler(Admin admin, MetricStore metricStore) {
    this.admin = admin;
    this.balancerConsole = BalancerConsole.create(admin);
    this.metricStore = metricStore;
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    if (channel.target().isEmpty()) return CompletableFuture.completedFuture(Response.NOT_FOUND);
    var taskId = channel.target().get();
    if (balancerConsole.taskPhase(taskId).isEmpty())
      return CompletableFuture.completedFuture(Response.NOT_FOUND);
    return CompletableFuture.completedFuture(progress(taskId));
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var balancerPostRequest = channel.request(TypeRef.of(BalancerPostRequest.class));
    var request =
        admin
            .topicNames(true)
            .thenCompose(admin::clusterInfo)
            .thenApply(
                currentClusterInfo ->
                    parsePostRequestWrapper(balancerPostRequest, currentClusterInfo))
            .toCompletableFuture()
            .join();
    var balancer =
        Utils.construct(request.balancerClasspath, Balancer.class, request.balancerConfig);
    synchronized (this) {
      var taskId = UUID.randomUUID().toString();
      var task =
          balancerConsole
              .launchRebalancePlanGeneration()
              .setTaskId(taskId)
              .setBalancer(balancer)
              .setAlgorithmConfig(request.algorithmConfig)
              .setClusterBeanSource(metricStore::clusterBean)
              .checkNoOngoingMigration(true)
              .generate();
      task.whenComplete(
          (result, error) -> {
            if (error != null)
              new RuntimeException("Failed to generate balance plan: " + taskId, error)
                  .printStackTrace();
          });
      taskMetadata.put(taskId, request);
      planGenerations.put(taskId, task);
      return CompletableFuture.completedFuture(new PostPlanResponse(taskId));
    }
  }

  @Override
  public CompletionStage<Response> put(Channel channel) {
    final var request = channel.request(TypeRef.of(BalancerPutRequest.class));
    final var taskId = request.id;
    final var taskPhase = balancerConsole.taskPhase(taskId);
    final var executorConfig = new Configuration(request.executorConfig);
    final var executor =
        Utils.construct(request.executor, RebalancePlanExecutor.class, executorConfig);

    if (taskPhase.isEmpty())
      throw new IllegalArgumentException("No such rebalance plan id: " + taskId);

    if (taskPhase.get() == BalancerConsole.TaskPhase.Executed)
      return CompletableFuture.completedFuture(Response.ACCEPT);

    // this method will fail if plan cannot be executed (lack of plan)
    var task =
        balancerConsole
            .launchRebalancePlanExecution()
            .setExecutor(executor)
            .setExecutionTimeout(Duration.ofHours(1))
            .checkNoOngoingMigration(true)
            .checkPlanConsistency(true)
            .execute(taskId);
    task.whenComplete(
        (ignore, error) -> {
          if (error != null)
            new RuntimeException("Failed to execute balance plan: " + taskId, error)
                .printStackTrace();
        });
    planExecutions.put(taskId, task);

    return CompletableFuture.completedFuture(new PutPlanResponse(taskId));
  }

  private PlanExecutionProgress progress(String taskId) {
    var contextCluster = taskMetadata.get(taskId).clusterInfo;
    var exception =
        (Function<BalancerConsole.TaskPhase, String>)
            (phase) ->
                switch (phase) {
                  case Searching, Searched, Executing, Executed ->
                  // No error message during the search & execution
                  null;
                  case SearchFailed -> planGenerations
                      .get(taskId)
                      .handle((plan, err) -> err != null ? err.toString() : null)
                      .toCompletableFuture()
                      .getNow(null);
                  case ExecutionFailed -> planExecutions
                      .get(taskId)
                      .handle((ignore, err) -> err != null ? err.toString() : null)
                      .toCompletableFuture()
                      .getNow(null);
                  default -> throw new IllegalStateException("Unknown state: " + phase);
                };
    var changes =
        (Function<Balancer.Plan, List<Change>>)
            (solution) ->
                ClusterInfo.findNonFulfilledAllocation(contextCluster, solution.proposal()).stream()
                    .map(
                        tp ->
                            Change.from(
                                contextCluster.replicas(tp), solution.proposal().replicas(tp)))
                    .toList();
    var report =
        (Supplier<PlanReport>)
            () ->
                Optional.ofNullable(
                        planGenerations
                            .get(taskId)
                            .toCompletableFuture()
                            .handle((res, err) -> res)
                            .getNow(null))
                    .map(
                        solution ->
                            new PlanReport(
                                changes.apply(solution),
                                MigrationCost.migrationCosts(
                                    contextCluster, solution.proposal(), solution.clusterBean())))
                    .orElse(null);
    var phase = balancerConsole.taskPhase(taskId).orElseThrow();
    return new PlanExecutionProgress(
        taskId,
        phase,
        taskMetadata.get(taskId).algorithmConfig.timeout(),
        taskMetadata.get(taskId).balancerClasspath,
        taskMetadata.get(taskId).algorithmConfig.clusterCostFunction().toString(),
        exception.apply(phase),
        report.get());
  }

  // visible for test
  static PostRequestWrapper parsePostRequestWrapper(
      BalancerPostRequest balancerPostRequest, ClusterInfo currentClusterInfo) {
    if (balancerPostRequest.timeout.isZero() || balancerPostRequest.timeout.isNegative())
      throw new IllegalArgumentException(
          "Illegal timeout, value should be positive integer: "
              + balancerPostRequest.timeout.getSeconds());

    return new PostRequestWrapper(
        balancerPostRequest.balancer,
        new Configuration(balancerPostRequest.balancerConfig),
        balancerPostRequest.parse(),
        currentClusterInfo);
  }

  static class BalancerPostRequest extends BalancerProblemFormat implements Request {}

  static class BalancerPutRequest implements Request {
    String id;
    String executor = StraightPlanExecutor.class.getName();
    Map<String, String> executorConfig = Map.of();
  }

  static class PostRequestWrapper {
    final String balancerClasspath;
    final Configuration balancerConfig;
    final AlgorithmConfig algorithmConfig;
    final ClusterInfo clusterInfo;

    PostRequestWrapper(
        String balancerClasspath,
        Configuration balancerConfig,
        AlgorithmConfig algorithmConfig,
        ClusterInfo clusterInfo) {
      this.balancerClasspath = balancerClasspath;
      this.balancerConfig = balancerConfig;
      this.algorithmConfig = algorithmConfig;
      this.clusterInfo = clusterInfo;
    }
  }

  static class Placement {

    final int brokerId;

    final String directory;

    final Optional<Long> size;

    Placement(Replica replica, Optional<Long> size) {
      this.brokerId = replica.brokerId();
      this.directory = replica.path();
      this.size = size;
    }
  }

  static class Change {
    final String topic;
    final int partition;
    final List<Placement> before;
    final List<Placement> after;

    static Change from(Collection<Replica> before, Collection<Replica> after) {
      if (before.size() == 0) throw new NoSuchElementException("Empty replica list was given");
      if (after.size() == 0) throw new NoSuchElementException("Empty replica list was given");
      var tp = before.stream().findAny().orElseThrow().topicPartition();
      if (!before.stream().allMatch(r -> r.topicPartition().equals(tp)))
        throw new IllegalArgumentException("Some replica come from different topic/partition");
      if (!after.stream().allMatch(r -> r.topicPartition().equals(tp)))
        throw new IllegalArgumentException("Some replica come from different topic/partition");
      return new Change(
          tp.topic(),
          tp.partition(),
          // only log the size from source replicas
          before.stream()
              .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
              .map(r -> new Placement(r, Optional.of(r.size())))
              .toList(),
          after.stream()
              .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
              .map(r -> new Placement(r, Optional.empty()))
              .toList());
    }

    Change(String topic, int partition, List<Placement> before, List<Placement> after) {
      this.topic = topic;
      this.partition = partition;
      this.before = before;
      this.after = after;
    }
  }

  // visible for testing
  static class PlanReport implements Response {

    final List<Change> changes;
    final List<MigrationCost> migrationCosts;

    PlanReport(List<Change> changes, List<MigrationCost> migrationCosts) {
      this.changes = changes;
      this.migrationCosts = migrationCosts;
    }
  }

  static class PostPlanResponse implements Response {
    final String id;

    PostPlanResponse(String id) {
      this.id = id;
    }
  }

  static class PutPlanResponse implements Response {
    final String id;

    PutPlanResponse(String id) {
      this.id = id;
    }

    @Override
    public int code() {
      return Response.ACCEPT.code();
    }
  }

  static class PlanExecutionProgress implements Response {
    final String id;
    final BalancerConsole.TaskPhase phase;
    final String exception;
    final PlanReport plan;
    final PlanConfiguration config;

    PlanExecutionProgress(
        String id,
        BalancerConsole.TaskPhase phase,
        Duration timeout,
        String balancer,
        String function,
        String exception,
        PlanReport plan) {
      this.id = id;
      this.phase = phase;
      this.exception = exception;
      this.plan = plan;
      this.config = new PlanConfiguration(balancer, function, timeout);
    }
  }

  static class PlanConfiguration implements Response {
    final String balancer;

    final String function;

    final Duration timeout;

    PlanConfiguration(String balancer, String function, Duration timeout) {
      this.balancer = balancer;
      this.function = function;
      this.timeout = timeout;
    }
  }

  // metricsStore creates many threads so we have to close it
  // in production, the web service will terminate all threads automatically.
  // in testing, all threads is still running even though the test get completed. hence, the test
  // case must close the
  // handle manually
  @Override
  public void close() {
    metricStore.close();
  }
}
