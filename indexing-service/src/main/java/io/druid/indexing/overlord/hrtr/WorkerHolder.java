/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.hrtr;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.io.AppendableByteArrayInputStream;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.InputStreamResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.concurrent.LifecycleLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.indexing.worker.WorkerHistoryItem;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.server.coordination.ChangeRequestHistory;
import io.druid.server.coordination.ChangeRequestsSnapshot;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 */
public class WorkerHolder
{
  private static final EmittingLogger log = new EmittingLogger(WorkerHolder.class);

  public static final TypeReference<ChangeRequestsSnapshot<WorkerHistoryItem>> WORKER_SYNC_RESP_TYPE_REF = new TypeReference<ChangeRequestsSnapshot<WorkerHistoryItem>>()
  {
  };

  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);

  private final Worker worker;
  private Worker disabledWorker;

  protected final AtomicBoolean disabled;

  protected final AtomicReference<Map<String, TaskAnnouncement>> tasksSnapshotRef = new AtomicReference<>(new ConcurrentHashMap<>());

  // CountDown happens when one sync with worker finishes successfully.
  protected final CountDownLatch initializationLatch = new CountDownLatch(1);

  private final LifecycleLock startStopLock = new LifecycleLock();

  private final AtomicReference<DateTime> lastCompletedTaskTime = new AtomicReference<>(DateTimes.nowUtc());
  private final AtomicReference<DateTime> blacklistedUntil = new AtomicReference<>();
  private final AtomicInteger continuouslyFailedTasksCount = new AtomicInteger(0);

  private volatile ChangeRequestHistory.Counter counter = null;
  private volatile long unstableStartTime = -1;
  private volatile int consecutiveFailedAttemptCount = 0;

  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final HttpRemoteTaskRunnerConfig config;
  private final ScheduledExecutorService workersSyncExec;

  private final Listener listener;

  public WorkerHolder(
      ObjectMapper smileMapper,
      HttpClient httpClient,
      HttpRemoteTaskRunnerConfig config,
      ScheduledExecutorService workersSyncExec,
      Listener listener,
      Worker worker
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.config = config;
    this.workersSyncExec = workersSyncExec;
    this.listener = listener;
    this.worker = worker;
    //worker holder is created disabled and gets enabled after first sync success.
    this.disabled = new AtomicBoolean(true);
  }

  public Worker getWorker()
  {
    return worker;
  }

  private Map<String, TaskAnnouncement> getRunningTasks()
  {
    return tasksSnapshotRef.get().entrySet().stream().filter(
        e -> e.getValue().getTaskStatus().isRunnable()
    ).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  private int getCurrCapacityUsed()
  {
    int currCapacity = 0;
    for (TaskAnnouncement taskAnnouncement : getRunningTasks().values()) {
      currCapacity += taskAnnouncement.getTaskResource().getRequiredCapacity();
    }
    return currCapacity;
  }

  private Set<String> getAvailabilityGroups()
  {
    Set<String> retVal = Sets.newHashSet();
    for (TaskAnnouncement taskAnnouncement : getRunningTasks().values()) {
      retVal.add(taskAnnouncement.getTaskResource().getAvailabilityGroup());
    }
    return retVal;
  }

  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime.get();
  }

  public DateTime getBlacklistedUntil()
  {
    return blacklistedUntil.get();
  }

  public void setLastCompletedTaskTime(DateTime completedTaskTime)
  {
    lastCompletedTaskTime.set(completedTaskTime);
  }

  public void setBlacklistedUntil(DateTime blacklistedUntil)
  {
    this.blacklistedUntil.set(blacklistedUntil);
  }

  public ImmutableWorkerInfo toImmutable()
  {
    Worker w = worker;
    if (disabled.get()) {
      if (disabledWorker == null) {
        disabledWorker = new Worker(
            worker.getScheme(),
            worker.getHost(),
            worker.getIp(),
            worker.getCapacity(),
            ""
        );
      }
      w = disabledWorker;
    }

    return new ImmutableWorkerInfo(
        w,
        getCurrCapacityUsed(),
        getAvailabilityGroups(),
        getRunningTasks().keySet(),
        lastCompletedTaskTime.get(),
        blacklistedUntil.get()
    );
  }

  public int getContinuouslyFailedTasksCount()
  {
    return continuouslyFailedTasksCount.get();
  }

  public void resetContinuouslyFailedTasksCount()
  {
    this.continuouslyFailedTasksCount.set(0);
  }

  public void incrementContinuouslyFailedTasksCount()
  {
    this.continuouslyFailedTasksCount.incrementAndGet();
  }

  public static URL makeWorkerURL(Worker worker, String path)
  {
    Preconditions.checkArgument(path.startsWith("/"), "path must start with '/': %s", path);

    try {
      return new URL(StringUtils.format("%s://%s%s", worker.getScheme(), worker.getHost(), path));
    }
    catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }

  public boolean assignTask(Task task)
  {
    if (disabled.get()) {
      log.info(
          "Received task[%s] assignment on worker[%s] when worker is disabled.",
          task.getId(),
          worker.getHost()
      );
      return false;
    }

    URL url = makeWorkerURL(worker, "/druid-internal/v1/worker/assignTask");
    int numTries = config.getAssignRequestMaxRetries();

    try {
      return RetryUtils.retry(
          () -> {
            try {
              final StatusResponseHolder response = httpClient.go(
                  new Request(HttpMethod.POST, url)
                      .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
                      .setContent(smileMapper.writeValueAsBytes(task)),
                  RESPONSE_HANDLER,
                  config.getAssignRequestHttpTimeout().toStandardDuration()
              ).get();

              if (response.getStatus().getCode() == 200) {
                return true;
              } else {
                throw new RE(
                    "Failed to assign task[%s] to worker[%s]. Response Code[%s] and Message[%s]. Retrying...",
                    task.getId(),
                    worker.getHost(),
                    response.getStatus().getCode(),
                    response.getContent()
                );
              }
            }
            catch (ExecutionException ex) {
              throw new RE(
                  ex,
                  "Request to assign task[%s] to worker[%s] failed. Retrying...",
                  task.getId(),
                  worker.getHost()
              );
            }
          },
          e -> !(e instanceof InterruptedException),
          numTries
      );
    }
    catch (Exception ex) {
      log.info("Not sure whether task[%s] was successfully assigned to worker[%s].", task.getId(), worker.getHost());
      return true;
    }
  }

  public void shutdownTask(String taskId)
  {
    URL url = makeWorkerURL(worker, StringUtils.format("/druid/worker/v1/task/%s/shutdown", taskId));

    try {
      RetryUtils.retry(
          () -> {
            try {
              final StatusResponseHolder response = httpClient.go(
                  new Request(HttpMethod.POST, url),
                  RESPONSE_HANDLER,
                  config.getShutdownRequestHttpTimeout().toStandardDuration()
              ).get();

              if (response.getStatus().getCode() == 200) {
                log.info(
                    "Sent shutdown message to worker: %s, status %s, response: %s",
                    worker.getHost(),
                    response.getStatus(),
                    response.getContent()
                );
                return null;
              } else {
                throw new RE("Attempt to shutdown task[%s] on worker[%s] failed.", taskId, worker.getHost());
              }
            }
            catch (ExecutionException e) {
              throw new RE(e, "Error in handling post to [%s] for task [%s]", worker.getHost(), taskId);
            }
          },
          e -> !(e instanceof InterruptedException),
          config.getShutdownRequestMaxRetries()
      );
    }
    catch (Exception ex) {
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      log.error("Failed to shutdown task[%s] on worker[%s] failed.", taskId, worker.getHost());
    }
  }

  public void start()
  {
    if (!startStopLock.canStart()) {
      throw new ISE("can't start.");
    }

    log.info("Starting WorkerHolder[hash=%d] for worker[%s].", this.hashCode(), worker.getHost());
    startStopLock.started();
    startStopLock.exitStart();

    addNextSyncToWorkQueue();
  }

  public void stop()
  {
    if (!startStopLock.canStop()) {
      throw new ISE("can't stop.");
    }

    log.info("Stopped WorkerHolder[hash=%d] for worker[%s].", this.hashCode(), worker.getHost());
  }

  public void waitForInitialization() throws InterruptedException
  {
    initializationLatch.await();
  }

  public void sync()
  {
    if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      log.info("Skipping sync for WorkerHolder[hash=%d] for worker[%s].", this.hashCode(), worker.getHost());
      return;
    }

    try {
      final String req;
      if (counter != null) {
        req = StringUtils.format(
            "/druid-internal/v1/worker?counter=%s&hash=%s&timeout=%s",
            counter.getCounter(),
            counter.getHash(),
            config.getSyncRequestTimeout()
        );
      } else {
        req = StringUtils.format(
            "/druid-internal/v1/worker?counter=-1&timeout=%s",
            config.getSyncRequestTimeout()
        );
      }

      URL url = makeWorkerURL(worker, req);
      BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();

      log.debug("Sending sync request to [%s] on URL [%s]", worker.getHost(), url);

      ListenableFuture<InputStream> future = httpClient.go(
          new Request(HttpMethod.GET, url)
              .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
              .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
          responseHandler,
          Duration.millis(config.getSyncRequestTimeout().toStandardDuration().getMillis() + 5000)
      );

      log.debug("Sent sync request to [%s] on URL [%s]", worker.getHost(), url);

      Futures.addCallback(
          future,
          new FutureCallback<InputStream>()
          {
            @Override
            public void onSuccess(InputStream stream)
            {
              if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                log.info(
                    "Skipping sync success response for WorkerHolder[hash=%d] for worker[%s].",
                    this.hashCode(),
                    worker.getHost()
                );
                return;
              }

              try {
                if (responseHandler.status == HttpServletResponse.SC_NO_CONTENT) {
                  log.debug("Received NO CONTENT from [%s]", worker.getHost());
                  consecutiveFailedAttemptCount = 0;
                  return;
                } else if (responseHandler.status != HttpServletResponse.SC_OK) {
                  onFailure(new RE("failed"));
                  return;
                } else {
                  log.debug("Received sync response from [%s]", worker.getHost());

                  ChangeRequestsSnapshot<WorkerHistoryItem> responseSnapshot = smileMapper.readValue(
                      stream,
                      WORKER_SYNC_RESP_TYPE_REF
                  );

                  log.debug("Finished reading sync response from [%s]", worker.getHost());

                  if (responseSnapshot.isResetCounter()) {
                    log.info(
                        "Worker [%s] requested resetCounter for reason [%s].",
                        worker.getHost(),
                        responseSnapshot.getResetCause()
                    );
                    counter = null;
                    return;
                  }

                  //Tasks that were added/updated
                  List<TaskAnnouncement> delta = new ArrayList<>();
                  boolean isWorkerDisabled = disabled.get();

                  if (counter == null) {
                    ConcurrentMap<String, TaskAnnouncement> newSnapshot = new ConcurrentHashMap<>();

                    for (WorkerHistoryItem change : responseSnapshot.getRequests()) {
                      if (change instanceof WorkerHistoryItem.TaskUpdate) {
                        TaskAnnouncement announcement = ((WorkerHistoryItem.TaskUpdate) change).getTaskAnnouncement();
                        newSnapshot.put(announcement.getTaskId(), announcement);
                        TaskAnnouncement old = tasksSnapshotRef.get().get(announcement.getTaskId());
                        if (old == null
                            || !old.getTaskLocation().equals(announcement.getTaskLocation())
                            || !old.getTaskStatus().equals(announcement.getTaskStatus())) {
                          delta.add(announcement);
                        }
                      } else if (change instanceof WorkerHistoryItem.Metadata) {
                        isWorkerDisabled = ((WorkerHistoryItem.Metadata) change).isDisabled();
                      } else {
                        throw new RE("Got unknown sync update[%s].", change.getClass().getName());
                      }
                    }

                    for (TaskAnnouncement announcement : tasksSnapshotRef.get().values()) {
                      if (!newSnapshot.containsKey(announcement.getTaskId()) && !announcement.getTaskStatus()
                                                                                             .isComplete()) {
                        delta.add(TaskAnnouncement.create(
                            announcement.getTaskId(),
                            announcement.getTaskResource(),
                            TaskStatus.failure("task suddenly disappeared."),
                            announcement.getTaskLocation()
                        ));
                      }
                    }

                    tasksSnapshotRef.set(newSnapshot);
                  } else {
                    for (WorkerHistoryItem change : responseSnapshot.getRequests()) {
                      if (change instanceof WorkerHistoryItem.TaskUpdate) {
                        TaskAnnouncement announcement = ((WorkerHistoryItem.TaskUpdate) change).getTaskAnnouncement();
                        tasksSnapshotRef.get().put(announcement.getTaskId(), announcement);
                        delta.add(announcement);
                      } else if (change instanceof WorkerHistoryItem.TaskRemoval) {
                        String taskId = ((WorkerHistoryItem.TaskRemoval) change).getTaskId();
                        TaskAnnouncement announcement = tasksSnapshotRef.get().remove(taskId);
                        if (announcement != null && !announcement.getTaskStatus().isComplete()) {
                          delta.add(TaskAnnouncement.create(
                              announcement.getTaskId(),
                              announcement.getTaskResource(),
                              TaskStatus.failure("task suddenly disappeared."),
                              announcement.getTaskLocation()
                          ));
                        }
                      } else if (change instanceof WorkerHistoryItem.Metadata) {
                        isWorkerDisabled = ((WorkerHistoryItem.Metadata) change).isDisabled();
                      } else {
                        throw new RE("Got unknown sync update[%s].", change.getClass().getName());
                      }
                    }
                  }

                  for (TaskAnnouncement announcement : delta) {
                    listener.taskAddedOrUpdated(announcement, WorkerHolder.this);
                  }

                  if (isWorkerDisabled != disabled.get()) {
                    disabled.set(isWorkerDisabled);
                    log.info("Worker[%s] disabled set to [%s].", isWorkerDisabled);
                  }

                  counter = responseSnapshot.getCounter();
                  consecutiveFailedAttemptCount = 0;
                  initializationLatch.countDown();
                }
              }
              catch (Exception ex) {
                String logMsg = StringUtils.nonStrictFormat(
                    "Error processing sync response from worker [%s]. Reason [%s]",
                    worker.getHost(),
                    ex.getMessage()
                );

                if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
                  log.error(ex, logMsg);
                } else {
                  log.info("Temporary Failure. %s", logMsg);
                  log.debug(ex, logMsg);
                }
              }
              finally {
                addNextSyncToWorkQueue();
              }
            }

            @Override
            public void onFailure(Throwable th)
            {
              if (!startStopLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                log.info(
                    "Skipping sync fail response for WorkerHolder[hash=%d] for worker[%s].",
                    this.hashCode(),
                    worker.getHost()
                );
                return;
              }

              try {
                String logMsg = StringUtils.nonStrictFormat(
                    "Error processing sync response from worker [%s]. Return code [%s], Reason: [%s]",
                    worker.getHost(),
                    responseHandler.status,
                    responseHandler.description
                );

                if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
                  log.error(th, logMsg);
                } else {
                  log.info("Temporary Failure. %s", logMsg);
                  log.debug(th, logMsg);
                }
              }
              finally {
                addNextSyncToWorkQueue();
              }
            }
          },
          workersSyncExec
      );
    }
    catch (Throwable th) {
      try {
        String logMsg = StringUtils.nonStrictFormat(
            "Fatal error while syncing with worker [%s].", worker.getHost()
        );

        if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
          log.makeAlert(th, logMsg).emit();
        } else {
          log.info("Temporary Failure. %s", logMsg);
          log.debug(th, logMsg);
        }
      }
      finally {
        addNextSyncToWorkQueue();
      }
    }
  }

  private void addNextSyncToWorkQueue()
  {
    if (consecutiveFailedAttemptCount > 0) {
      long sleepMillis = RetryUtils.nextRetrySleepMillis(consecutiveFailedAttemptCount);
      log.info("Scheduling next syncup in [%d] millis from worker [%s].", sleepMillis, worker.getHost());
      workersSyncExec.schedule(
          this::sync,
          sleepMillis,
          TimeUnit.MILLISECONDS
      );
    } else {
      workersSyncExec.execute(this::sync);
    }
  }

  private boolean incrementFailedAttemptAndCheckUnstabilityTimeout()
  {
    if (consecutiveFailedAttemptCount > 0
        && (System.currentTimeMillis() - unstableStartTime) > config.getServerUnstabilityTimeout()
                                                                    .toStandardDuration()
                                                                    .getMillis()) {
      return true;
    }

    if (consecutiveFailedAttemptCount++ == 0) {
      unstableStartTime = System.currentTimeMillis();
    }

    return false;
  }

  private static class BytesAccumulatingResponseHandler extends InputStreamResponseHandler
  {
    private int status;
    private String description;

    @Override
    public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
    {
      status = response.getStatus().getCode();
      description = response.getStatus().getReasonPhrase();
      return ClientResponse.unfinished(super.handleResponse(response).getObj());
    }
  }

  public interface Listener
  {
    void taskAddedOrUpdated(TaskAnnouncement announcement, WorkerHolder workerHolder);
  }
}
