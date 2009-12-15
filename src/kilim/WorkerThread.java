/* Copyright (c) 2006, Sriram Srinivasan
 *
 * You may distribute this software under the terms of the license 
 * specified in the file "License"
 */

package kilim;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerThread extends Thread {
    Task runningTask;
    /**
     * A list of tasks that prefer to run only on this thread. This is used
     * by kilim.ReentrantLock and Task to ensure that lock.release() is
     * done on the same thread as lock.acquire()
     */
    PriorityQueue<Task> tasks = new PriorityQueue<Task>(10, new Comparator<Task>() {

		public int compare(Task o1, Task o2) {
			if (o1.priority < o2.priority)
				return -1;
			else if (o1.priority == o2.priority) 
				return 0;
			else
				return 1;
		}});
    Scheduler scheduler;

    public int numResumes = 0;
    
    WorkerThread(Scheduler ascheduler) {
    	setDaemon(true);
        scheduler=ascheduler;
    }
    
    public void run() {
        try {
        while (true) {
            Task t = scheduler.getNextTask(this); // blocks until task available
            if (t == null) break; // scheduler shut down
            runningTask = t;
            t._runExecute(this);
        }
        } catch (Throwable ex) {
            ex.printStackTrace();
            System.out.println(runningTask);
        }
    }

    public Task getCurrentTask() {
        return runningTask;
    }
    
    public synchronized void addRunnableTask(Task t) {
        assert t.preferredResumeThread == null || t.preferredResumeThread == this : "Task given to wrong thread";
        tasks.add(t);
        notify();
    }

    public synchronized boolean hasTasks() {
    	return tasks.size() > 0;
    }
    public synchronized Task getNextTask() {
        Task task = tasks.peek();
        if (task != null) {
        	tasks.remove(task);
        }
        return task;
    }
        
    public synchronized void waitForMsgOrSignal() {
        try {
            if (tasks.size() == 0) {
                wait(100); // sleep 100ms
            }
        } catch (InterruptedException ignore) {}
    }
}
