/** Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zookeeper.book.curator;


/**
 * Application master using the curator framework. This code
 * example uses the following curator features:
 * 1- The curator zookeeper client;
 * 2- The fluent API to zookeeper operations;
 * 3- The leader latch primitive;
 * 4- The children cache implementation to hold and manage 
 *    workers and tasks.
 */

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class CuratorMasterLatch implements Closeable, LeaderLatchListener {
    private static final Logger LOG = LoggerFactory.getLogger(CuratorMasterLatch.class);
    
    private String myId;
    private CuratorFramework client;
    private final LeaderLatch leaderLatch;
    private final PathChildrenCache workersCache;
    private final PathChildrenCache tasksCache;
            
    
    /**
     * Creates a new Curator client, setting the the retry policy
     * to ExponentialBackoffRetry.
     * 
     * @param myId
     *          master identifier
     * @param hostPort
     *          list of zookeeper servers comma-separated
     * @param retryPolicy
     *          Curator retry policy
     */
    public CuratorMasterLatch(String myId, String hostPort, RetryPolicy retryPolicy){
        LOG.info( myId + ": " + hostPort );
        
        this.myId = myId;
        this.client = CuratorFrameworkFactory.newClient(hostPort, 
                retryPolicy);
        this.leaderLatch = new LeaderLatch(this.client, "/master", myId);
        this.workersCache = new PathChildrenCache(this.client, "/workers", true);
        this.tasksCache = new PathChildrenCache(this.client, "/tasks", true);
    }
    
    public void startZK(){
        client.start();
    }
    
    public void bootstrap()
    throws Exception {
        client.create().forPath("/workers", new byte[0]);
        client.create().forPath("/assign", new byte[0]);
        client.create().forPath("/tasks", new byte[0]);
        client.create().forPath("/status", new byte[0]);
    }
    
    public void runForMaster() 
    throws Exception {
        /*
         * Register listeners
         */
        client.getCuratorListenable().addListener(masterListener);
        client.getUnhandledErrorListenable().addListener(errorsListener);
        
        
        /*
         * Start master election
         */
        LOG.info( "Starting master selection: " + myId);
        leaderLatch.addListener(this);
        leaderLatch.start();
    }
    
   
    /**
     * Waits until it becomes leader.
     * 
     * @throws InterruptedException
     */
    public void awaitLeadership()

    throws InterruptedException, EOFException {
        leaderLatch.await();
    }
    
    
    CountDownLatch recoveryLatch = new CountDownLatch(0);
    
    /**
     * Executed when client becomes leader.
     * 
     * @throws Exception
     */
    @Override
    public void isLeader() {            
        /*
         * Start workersCache
         */
        try{
            workersCache.getListenable().addListener(workersCacheListener);
            workersCache.start();
        
            (new RecoveredAssignments(client.getZookeeperClient().getZooKeeper())).recover( new RecoveryCallback() {
                public void recoveryComplete (int rc, List<String> tasks) {
                    try{
                        if(rc == RecoveryCallback.FAILED) {
                            LOG.warn("Recovery of assigned tasks failed.");
                        } else {
                            LOG.info( "Assigning recovered tasks" );
                            recoveryLatch = new CountDownLatch(tasks.size());
                            assignTasks(tasks);
                        }
                    
                        new Thread( new Runnable() {
                            public void run() {
                                try{
                                    /*
                                     * Wait until recovery is complete
                                     */
                                    recoveryLatch.await();
                            
                                    /*
                                     * Starts tasks cache
                                     */
                                    tasksCache.getListenable().addListener(tasksCacheListener);
                                    tasksCache.start();
                                } catch (Exception e) {
                                    LOG.warn("Exception while assigning and getting tasks.", e  );
                                }
                            }
                        }).start();
                    } catch (Exception e) {
                        LOG.error("Exception while executing the recovery callback", e);
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("Exception when starting leadership", e);
        }
    }
    
    @Override
    public void notLeader() {
        LOG.info( "Lost leadership" );
        try{
            close();
        } catch (IOException e) {
            LOG.warn("Exception while closing", e);
        }
    }
    
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }
    
    /*
     * We use one main listener for the master. The listener processes
     * callback and watch events from various calls we make. Note that
     * many of the events related to workers and tasks are processed
     * directly by the workers cache and the tasks cache.
     */
    CuratorListener masterListener = new CuratorListener() {
        public void eventReceived(CuratorFramework client, CuratorEvent event){
            try{
                LOG.info("Event path: " + event.getPath());
                switch (event.getType()) { 
                case CHILDREN:
                    if(event.getPath().contains("/assign")) {
                        LOG.info("Succesfully got a list of assignments: " 
                                + event.getChildren().size() 
                                + " tasks");
                        /*
                         * Delete the assignments of the absent worker
                         */
                        for(String task : event.getChildren()){
                            deleteAssignment(event.getPath() + "/" + task);
                        }
                        
                        /*
                         * Delete the znode representing the absent worker
                         * in the assignments.
                         */
                        deleteAssignment(event.getPath());
                        
                        /*
                         * Reassign the tasks.
                         */
                        assignTasks(event.getChildren());
                    } else {
                        LOG.warn("Unexpected event: " + event.getPath());
                    }
                
                    break;
                case CREATE:
                    /*
                     * Result of a create operation when assigning
                     * a task.
                     */
                    if(event.getPath().contains("/assign")) {
                        LOG.info("Task assigned correctly: " + event.getName());
                        deleteTask(event.getPath().substring(event.getPath().lastIndexOf('-') + 1));
                    }
                
                    break;
                case DELETE:
                    /*
                     * We delete znodes in two occasions:
                     * 1- When reassigning tasks due to a faulty worker;
                     * 2- Once we have assigned a task, we remove it from
                     *    the list of pending tasks. 
                     */
                    if(event.getPath().contains("/tasks")) {
                        LOG.info("Result of delete operation: " + event.getResultCode() + ", " + event.getPath());
                    } else if(event.getPath().contains("/assign")) {
                        LOG.info("Task correctly deleted: " + event.getPath());
                        break;
                    }
                
                    break;
                case WATCHED:
                    // There is no case implemented currently.
                    
                    break;
                default:
                    LOG.error("Default case: " + event.getType());
                }
            } catch (Exception e) {
                LOG.error("Exception while processing event.", e);
                try{
                    close();
                } catch (IOException ioe) {
                    LOG.error("IOException while closing.", ioe);
                }
            }
        };
    };
    
    PathChildrenCacheListener workersCacheListener = new PathChildrenCacheListener() {
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            if(event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                /*
                 * Obtain just the worker's name
                 */
                try{
                    getAbsentWorkerTasks(event.getData().getPath().replaceFirst("/workers/", ""));
                } catch (Exception e) {
                    LOG.error("Exception while trying to re-assign tasks", e);
                }
            } 
        }
    };
    
    PathChildrenCacheListener tasksCacheListener = new PathChildrenCacheListener() {
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                try{
                    assignTask(event.getData().getPath().replaceFirst("/tasks/", ""),
                            event.getData().getData());
                } catch (Exception e) {
                    LOG.error("Exception when assigning task.", e);
                }   
            }
        }
    };
    
    private void getAbsentWorkerTasks(String worker)
    throws Exception { 
        /*
         * Get assigned tasks
         */
        client.getChildren().inBackground().forPath("/assign/" + worker); 
    }
    
    void deleteAssignment(String path)
    throws Exception {
        /*
         * Delete assignment
         */
        LOG.info( "Deleting assignment: {}", path );
        client.delete().inBackground().forPath(path);
    }
    
    /*
     * Random variable we use to select a worker to perform a pending task.
     */
    Random rand = new Random(System.currentTimeMillis());
    
    void assignTasks (List<String> tasks)
    throws Exception {
      for(String task : tasks) {
          assignTask(task, client.getData().forPath("/tasks/" + task));
      }
    }

    void assignTask (String task, byte[] data) 
            throws Exception {
        /*
         * Choose worker at random.
         */
        //String designatedWorker = workerList.get(rand.nextInt(workerList.size()));
        List<ChildData> workersList = workersCache.getCurrentData(); 
            
        LOG.info("Assigning task {}, data {}", task, new String(data));
        
        String designatedWorker = workersList.get(rand.nextInt(workersList.size())).getPath().replaceFirst("/workers/", "");
                    
        /*
         * Assign task to randomly chosen worker.
         */
        String path = "/assign/" + designatedWorker + "/" + task;
        createAssignment(path, data);
    }

    
    /**
     * Creates an assignment.
     * 
     * @param path
     *          path of the assignment
     */
    void createAssignment(String path, byte[] data)
    throws Exception {
        /*
         * The default ACL is ZooDefs.Ids#OPEN_ACL_UNSAFE
         */
        client.create().withMode(CreateMode.PERSISTENT).inBackground().forPath(path, data); 
    }
    
    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteTask(String number) 
            throws Exception {
        LOG.info("Deleting task: {}", number);
        client.delete().inBackground().forPath("/tasks/task-" + number);
        recoveryLatch.countDown();
    }
    
    @Override
    public void close() 
            throws IOException
    {
        LOG.info( "Closing" );
        leaderLatch.close();
        client.close();
    }
    
    UnhandledErrorListener errorsListener = new UnhandledErrorListener() {
        public void unhandledError(String message, Throwable e) {
            LOG.error("Unrecoverable error: " + message, e);
            try {
                close();
            } catch (IOException ioe) {
                LOG.warn( "Exception when closing.", ioe );
            }
        }
    };
    
    
    public static void main (String[] args) {
        try{
            CuratorMasterLatch master = new CuratorMasterLatch(args[0], args[1], 
                    new ExponentialBackoffRetry(1000, 5));
            master.startZK();
            master.bootstrap();
            master.runForMaster();

            Thread.sleep(Long.MAX_VALUE);

        } catch (Exception e) {
            LOG.error("Exception while running curator master.", e);
        }
    }
}