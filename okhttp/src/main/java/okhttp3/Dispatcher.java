/*
 * Copyright (C) 2013 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package okhttp3;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import okhttp3.RealCall.AsyncCall;
import okhttp3.internal.Util;

/**
 * Policy on when async requests are executed.
 *
 * <p>Each dispatcher uses an {@link ExecutorService} to run calls internally. If you supply your
 * own executor, it should be able to run {@linkplain #getMaxRequests the configured maximum} number
 * of calls concurrently.
 *
 * 异步请求被执行时采取策略
 *
 * 每个调度器内部使用一个线程池执行请求，如果你提供了你自己的线程池，它应该能够执行getMaxRequests方法来获得配置好的允许并发执行的最大请求数
 */
public final class Dispatcher {
  private int maxRequests = 64;
  private int maxRequestsPerHost = 5;
  private @Nullable Runnable idleCallback;

  /**
   * Executes calls. Created lazily.
   * 执行请求，懒加载
   */
  private @Nullable ExecutorService executorService;

  /** Ready async calls in the order they'll be run. */
  private final Deque<AsyncCall> readyAsyncCalls = new ArrayDeque<>();

  /** Running asynchronous calls. Includes canceled calls that haven't finished yet. */
  private final Deque<AsyncCall> runningAsyncCalls = new ArrayDeque<>();

  /**
   * Running synchronous calls. Includes canceled calls that haven't finished yet.
   * 运行中的同步请求队列，包括已经被取消但还没有完成的请求
   */
  private final Deque<RealCall> runningSyncCalls = new ArrayDeque<>();

  public Dispatcher(ExecutorService executorService) {
    this.executorService = executorService;
  }

  public Dispatcher() {
  }

  public synchronized ExecutorService executorService() {
    if (executorService == null) {
	  //线程池配置：没有核心线程、允许最多Integer.MAX_VALUE个线程同时执行、非核心线程闲置60秒回收
      executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(), Util.threadFactory("OkHttp Dispatcher", false));
    }
    return executorService;
  }

  /**
   * Set the maximum number of requests to execute concurrently. Above this requests queue in
   * memory, waiting for the running calls to complete.
   *
   * <p>If more than {@code maxRequests} requests are in flight when this is invoked, those requests
   * will remain in flight.
   */
  public synchronized void setMaxRequests(int maxRequests) {
    if (maxRequests < 1) {
      throw new IllegalArgumentException("max < 1: " + maxRequests);
    }
    this.maxRequests = maxRequests;
    promoteCalls();
  }

  public synchronized int getMaxRequests() {
    return maxRequests;
  }

  /**
   * Set the maximum number of requests for each host to execute concurrently. This limits requests
   * by the URL's host name. Note that concurrent requests to a single IP address may still exceed
   * this limit: multiple hostnames may share an IP address or be routed through the same HTTP
   * proxy.
   *
   * <p>If more than {@code maxRequestsPerHost} requests are in flight when this is invoked, those
   * requests will remain in flight.
   *
   * <p>WebSocket connections to hosts <b>do not</b> count against this limit.
   */
  public synchronized void setMaxRequestsPerHost(int maxRequestsPerHost) {
    if (maxRequestsPerHost < 1) {
      throw new IllegalArgumentException("max < 1: " + maxRequestsPerHost);
    }
    this.maxRequestsPerHost = maxRequestsPerHost;
    promoteCalls();
  }

  public synchronized int getMaxRequestsPerHost() {
    return maxRequestsPerHost;
  }

  /**
   * Set a callback to be invoked each time the dispatcher becomes idle (when the number of running
   * calls returns to zero).
   *
   * <p>Note: The time at which a {@linkplain Call call} is considered idle is different depending
   * on whether it was run {@linkplain Call#enqueue(Callback) asynchronously} or
   * {@linkplain Call#execute() synchronously}. Asynchronous calls become idle after the
   * {@link Callback#onResponse onResponse} or {@link Callback#onFailure onFailure} callback has
   * returned. Synchronous calls become idle once {@link Call#execute() execute()} returns. This
   * means that if you are doing synchronous calls the network layer will not truly be idle until
   * every returned {@link Response} has been closed.
   */
  public synchronized void setIdleCallback(@Nullable Runnable idleCallback) {
    this.idleCallback = idleCallback;
  }

  synchronized void enqueue(AsyncCall call) {
  	//如果正在运行的异步请求队列的数量小于最大限制且当前请求的host是小于5个请求的host，则执行当前请求并把当前请求放入正在运行的异步请求队列
  	//否则放入就绪的异步请求队列等待执行
    if (runningAsyncCalls.size() < maxRequests && runningCallsForHost(call) < maxRequestsPerHost) {
      runningAsyncCalls.add(call);
      executorService().execute(call);
    } else {
      readyAsyncCalls.add(call);
    }
  }

  /**
   * Cancel all calls currently enqueued or executing. Includes calls executed both {@linkplain
   * Call#execute() synchronously} and {@linkplain Call#enqueue asynchronously}.
   */
  public synchronized void cancelAll() {
    for (AsyncCall call : readyAsyncCalls) {
      call.get().cancel();
    }

    for (AsyncCall call : runningAsyncCalls) {
      call.get().cancel();
    }

    for (RealCall call : runningSyncCalls) {
      call.cancel();
    }
  }

  private void promoteCalls() {
  	//如果正在运行的异步请求队列数量大于最大限制，则返回
    if (runningAsyncCalls.size() >= maxRequests) return; // Already running max capacity.
    //如果就绪的异步请求队列为空，则返回
    if (readyAsyncCalls.isEmpty()) return; // No ready calls to promote.

	//遍历就绪的异步请求队列
    for (Iterator<AsyncCall> i = readyAsyncCalls.iterator(); i.hasNext(); ) {
      AsyncCall call = i.next();

	  //如果满足当前调用请求主机数(runningCallsForHost)小于最大请求主机数(maxRequestPerHost，5个)，则把当前请求放入正在运行的异步请求队列并开始执行
      if (runningCallsForHost(call) < maxRequestsPerHost) {
        i.remove();
        runningAsyncCalls.add(call);
        executorService().execute(call);
      }

	  //如果正在运行的异步请求数量达到最大限制，则返回
      if (runningAsyncCalls.size() >= maxRequests) return; // Reached max capacity.
    }
  }

  /** Returns the number of running calls that share a host with {@code call}. */
  private int runningCallsForHost(AsyncCall call) {
    int result = 0;
    for (AsyncCall c : runningAsyncCalls) {
      if (c.get().forWebSocket) continue;
      if (c.host().equals(call.host())) result++;
    }
    return result;
  }

  /** Used by {@code Call#execute} to signal it is in-flight. */
  synchronized void executed(RealCall call) {
    runningSyncCalls.add(call);
  }

  /** Used by {@code AsyncCall#run} to signal completion. */
  void finished(AsyncCall call) {
    finished(runningAsyncCalls, call, true);
  }

  /** Used by {@code Call#execute} to signal completion. */
  void finished(RealCall call) {
    finished(runningSyncCalls, call, false);
  }

  private <T> void finished(Deque<T> calls, T call, boolean promoteCalls) {
    int runningCallsCount;
    Runnable idleCallback; //闲置接口
    synchronized (this) {
	  //将运行队列列表中的当前请求清除掉
      if (!calls.remove(call)) throw new AssertionError("Call wasn't in-flight!");
	  //将等待队列的请求加入运行队列并开始执行，只会在异步方法中调用
      if (promoteCalls) promoteCalls();
	  //获取运行中的同步请求队列和运行中的异步请求队列的数量之和
      runningCallsCount = runningCallsCount();
      idleCallback = this.idleCallback;
    }

	//如果上面得到的数量为0，说明目前没有正在执行的请求
	//此时如果闲置接口(idleCallback)不为null，则执行闲置接口的方法
    if (runningCallsCount == 0 && idleCallback != null) {
      idleCallback.run();
    }
  }

  /** Returns a snapshot of the calls currently awaiting execution. */
  public synchronized List<Call> queuedCalls() {
    List<Call> result = new ArrayList<>();
    for (AsyncCall asyncCall : readyAsyncCalls) {
      result.add(asyncCall.get());
    }
    return Collections.unmodifiableList(result);
  }

  /** Returns a snapshot of the calls currently being executed. */
  public synchronized List<Call> runningCalls() {
    List<Call> result = new ArrayList<>();
    result.addAll(runningSyncCalls);
    for (AsyncCall asyncCall : runningAsyncCalls) {
      result.add(asyncCall.get());
    }
    return Collections.unmodifiableList(result);
  }

  public synchronized int queuedCallsCount() {
    return readyAsyncCalls.size();
  }

  public synchronized int runningCallsCount() {
    return runningAsyncCalls.size() + runningSyncCalls.size();
  }
}
