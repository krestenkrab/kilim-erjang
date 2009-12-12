/* Copyright (c) 2006, Sriram Srinivasan
 *
 * You may distribute this software under the terms of the license 
 * specified in the file "License"
 */

package kilim;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;

/** 
 * This is a basic FIFO Executor. It maintains a list of
 * runnable tasks and hands them out to WorkerThreads. Note that
 * we don't maintain a list of all tasks, but we will at some point
 * when we introduce monitoring/watchdog services. 
 * Paused tasks are not GC'd because their PauseReasons ought to be 
 * registered with some other live object.
 * 
 */
public class Scheduler {
    static Scheduler defaultScheduler = null;
    static int defaultNumberThreads;
    LinkedList<WorkerThread> allThreads = new LinkedList<WorkerThread>();
    LinkedList<WorkerThread> waitingThreads = new LinkedList<WorkerThread>();
    protected boolean shutdown = false;
    // protected RingQueue<Task> runnableTasks = new RingQueue<Task>(1000);
    protected PriorityQueue<Task> runnableTasks = new PriorityQueue<Task>(1000, new Comparator<Task>() {

		public int compare(Task o1, Task o2) {
			if (o1.priority < o2.priority)
				return -1;
			else if (o1.priority == o2.priority) 
				return 0;
			else
				return 1;
		}});

    static {
        String s = System.getProperty("kilim.Scheduler.numThreads");
        if (s != null) {
            try {
                defaultNumberThreads = Integer.parseInt(s);
            } catch(Exception e) {}
        }
        if (defaultNumberThreads == 0) {
            defaultNumberThreads = Runtime.getRuntime().availableProcessors();
        }
    }
    protected Scheduler() {}
    
    public Scheduler(int numThreads) {
        for (int i = 0; i < numThreads; i++) {
            WorkerThread wt = new WorkerThread(this);
            allThreads.add(wt);
            waitingThreads.add(wt);
            wt.start();
        }
    }
    
    /**
     * Schedule a task to run. It is the task's job to ensure that
     * it is not scheduled when it is runnable.
     * ensure that 
     */
    public void schedule(Task t) {
        WorkerThread wt = null;
        
        synchronized(this) {
            assert t.running == true :  "Task " + t + " scheduled even though running is false";
            runnableTasks.add(t);
            if (!waitingThreads.isEmpty())
                wt = waitingThreads.poll();
        }
        if (wt != null) {
            synchronized(wt) {
                wt.notify();
            }
        }
    }
    
    public void shutdown() {
        synchronized(this) {
            shutdown = true;
            notifyAll();
        }
    }
    
    Task getNextTask(WorkerThread wt) {
        while (true) {
            Task t = null;
            WorkerThread prefThread = null;
            ///////////////
            synchronized(this) {
                if (shutdown) return null;

                if ((t = wt.getNextTask()) != null) {
                    return t;
                }

                t = runnableTasks.peek();
                if (t == null) {
                    waitingThreads.add(wt);
                } else {
                	Task tt = runnableTasks.remove();
                	assert t == tt : "queue not in order?"; 
                    prefThread = t.preferredResumeThread;
                }
            }
            /////////////
            if (t == null) {
                wt.waitForMsgOrSignal();                
            } else if (prefThread == null || prefThread == wt) {
                assert t.currentThread == null: " Task " + t + " already running";
                return t;
            } else {
                prefThread.addRunnableTask(t);
            }
        }
    }

    public synchronized static Scheduler getDefaultScheduler() {
        if (defaultScheduler == null) {
            defaultScheduler = new Scheduler(defaultNumberThreads);
        }
        return defaultScheduler;
    }
    
    public static void setDefaultScheduler(Scheduler s) {
        defaultScheduler = s;
    }

	public void dump() {
		System.out.println(runnableTasks);
//		for (WorkerThread w: allThreads) {
//			w.dumpStack();
//		}
	}

	public static boolean isRunnable(Task task) {
		Scheduler s = defaultScheduler;
		synchronized (s) {
			if (s.runnableTasks.contains(task)) {
				return true;
			}
			for (WorkerThread wt: s.allThreads) {
				if (wt.tasks.contains(task)) return true;
			}
		}
		return false;
	}
}
