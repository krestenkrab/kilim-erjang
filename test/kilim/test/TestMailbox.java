/* Copyright (c) 2006, Sriram Srinivasan
 *
 * You may distribute this software under the terms of the license 
 * specified in the file "License"
 */

package kilim.test;

import java.util.HashMap;
import java.util.HashSet;

import junit.framework.TestCase;
import kilim.Event;
import kilim.EventPublisher;
import kilim.Mailbox;
import kilim.Pausable;
import kilim.Task;

public class TestMailbox extends TestCase { 
    public void testThread() {
        final Mailbox<Msg> mb = new Mailbox<Msg>();
        final int nThreads = 30;
        final int nTimes = 1000;
        final int nMsgs = nThreads * nTimes;
        for (int i = 0; i < nThreads ; i++) {
            final int id = i;
            new Thread() {
                public void run() {
                    for (int j = 0; j < nTimes; j++) {
                        mb.putnb(new Msg(id, j));
                        Thread.yield();
                    }
                }
            }.start();
        }
        
        
        HashMap<Integer, Integer> lastRcvd = new HashMap<Integer,Integer>();
        int nNewThreads = 0;
        for (int i = 0; i < nMsgs; i++) {
            Msg m = mb.getb();
            // assert that the number received is one more than the last received 
            // from that thread.
            Integer last = lastRcvd.put(m.tid, m.num);
            if (last == null) {
                nNewThreads++;
            } else {
                assertTrue(m.num == last.intValue() + 1);
            }
        }
        assertTrue(nNewThreads == nThreads);
        // Make sure we have heard from all threads
        assertTrue(lastRcvd.keySet().size() == nThreads);
        int lastVal = nTimes - 1;
        for(Integer tid : lastRcvd.keySet()) {
            Integer v = lastRcvd.get(tid);
            assertTrue(v != null);
            assertTrue(v.intValue() == lastVal);
        }
        try {Thread.sleep(1000);} catch (InterruptedException ignore) {}
        // Make sure there are no extra messages floating around.
        assertTrue(mb.getnb() == null); 
    }
    
    public void testTasks() {
        Mailbox<Msg> mb = new Mailbox<Msg>();
        
        final int nTasks = 100;
        final int nTimes = 1000;
        final int nMsgs = nTasks * nTimes;
        
        for (int i = 0; i < nTasks ; i++) {
            TaskMB t = new TaskMB(/*mainmb=*/mb);
            t.start();
            t.mymb.putnb(new Msg(i, nTimes));
        }
        
        HashMap<Integer, Integer> lastRcvd = new HashMap<Integer,Integer>();
        int nNewThreads = 0;
        for (int i = 0; i < nMsgs; i++) {
            Msg m = mb.getb();
            // assert that the number received is one more than the last received 
            // from that thread.
            Integer last = lastRcvd.put(m.tid, m.num);
            if (last == null) {
                nNewThreads++;
            } else {
                assertTrue(m.num == last.intValue() + 1);
            }
        }
        assertTrue(nNewThreads == nTasks);
        // Make sure we have heard from all threads
        assertTrue(lastRcvd.keySet().size() == nTasks);
        int lastVal = nTimes - 1;
        for(Integer tid : lastRcvd.keySet()) {
            Integer v = lastRcvd.get(tid);
            assertTrue(v != null);
            assertTrue(v.intValue() == lastVal);
        }
        try {Thread.sleep(1000);} catch (InterruptedException ignore) {}
        // Make sure there are no extra messages floating around.
        assertTrue(mb.getnb() == null); 
    }
    
}

class Msg {
    int tid; // thread or task id
    int num;
    Msg(){}
    Msg(int id, int n) {tid = id; num = n;}
    public String toString() {
        return "Msg(" + tid + "," + num + ")";
    }
};


class TaskMB extends Task {
    Mailbox<Msg> mymb;
    Mailbox<Msg> mainmb;
    
    TaskMB(Mailbox<Msg> ms) {
        mymb = new Mailbox<Msg>();
        mainmb = ms;
    }
    
    public void execute() throws Pausable {
        Msg m = mymb.get();
        assert m != null : "task rcvd null msg";
        int id = m.tid; // Receive this task's id
        int n = m.num;  // Receive the number of times we have to loop
        
        for (int i = 0; i < n; i++) {
            mainmb.put(new Msg(id, i));
            if (i % 10 == 0) {
                Task.yield();
            }
        }
    }
}


