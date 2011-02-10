/* Copyright (c) 2006, Sriram Srinivasan
 *
 * You may distribute this software under the terms of the license 
 * specified in the file "License"
 */

package kilim;

import java.util.LinkedList;
import java.util.TimerTask;

/**
 * This is a typed buffer that supports multiple producers and a single
 * consumer. It is the basic construct used for tasks to interact and
 * synchronize with each other (as opposed to direct java calls or static member
 * variables). put() and get() are the two essential functions.
 * 
 * We use the term "block" to mean thread block, and "pause" to mean fiber
 * pausing. The suffix "nb" on some methods (such as getnb()) stands for
 * non-blocking.
 */

public class Mailbox<T> implements PauseReason, EventPublisher {
	// TODO. Give mbox a config name and id and make monitorable
	T[] msgs;
	private int iprod = 0; // producer index
	private int icons = 0; // consumer index;
	private int numMsgs = 0;
	private int maxMsgs = 300;
	MessageConsumer sink;

	// FIX: I don't like this event design. The only good thing is that
	// we don't create new event objects every time we signal a client
	// (subscriber) that's blocked on this mailbox.
	public static final int SPACE_AVAILABLE = 1;
	public static final int MSG_AVAILABLE = 2;
	public static final int TIMED_OUT = 3;
	public static final Event spaceAvailble = new Event(MSG_AVAILABLE);
	public static final Event messageAvailable = new Event(SPACE_AVAILABLE);
	public static final Event timedOut = new Event(TIMED_OUT);

	LinkedList<MessageProducer> pendingProducers = new LinkedList<MessageProducer>();

	// DEBUG stuff
	// To do: move into monitorable stat object
	/*
	 * public int nPut = 0; public int nGet = 0; public int nWastedPuts = 0;
	 * public int nWastedGets = 0;
	 */
	public Mailbox() {
		this(10);
	}

	public Mailbox(int initialSize) {
		this(initialSize, Integer.MAX_VALUE);
	}

	@SuppressWarnings("unchecked")
	public Mailbox(int initialSize, int maxSize) {
		if (initialSize > maxSize)
			throw new IllegalArgumentException("initialSize: " + initialSize
					+ " cannot exceed maxSize: " + maxSize);
		msgs = (T[]) new Object[initialSize];
		maxMsgs = maxSize;
	}

	/**
	 * Non-blocking, nonpausing get.
	 * 
	 * @param eo
	 *            . If non-null, registers this observer and calls it with a
	 *            MessageAvailable event when a put() is done.
	 * @return buffered message if there's one, or null
	 */
	public T get(MessageConsumer eo) {
		T msg;
		MessageProducer producer = null;
		synchronized (this) {
			int n = numMsgs;
			if (n > 0) {
				int ic = icons;
				msg = msgs[ic];
				msgs[ic] = null;
				icons = (ic + 1) % msgs.length;
				numMsgs = n - 1;

				assert msg != null : "received null message!";
				
				if (pendingProducers.size() > 0) {
					producer = pendingProducers.poll();
				}
			} else {
				msg = null;
				addMessageConsumer(eo);
			}
		}
		if (producer != null) {
			producer.spaceAvailable(this);
		}
		return msg;
	}

	/**
	 * @return non-null message.
	 * @throws Pausable
	 */
	public void untilHasMessage() throws Pausable {
		while (hasMessage(Task.getCurrentTask()) == false) {
			Task.pause(this);
		}
	}

	/**
	 * @return non-null message.
	 * @throws Pausable
	 */
	public void untilHasMessages(int num) throws Pausable {
		while (hasMessages(num, Task.getCurrentTask()) == false) {
			Task.pause(this);
		}
	}

	/**
	 * @return non-null message.
	 * @throws Pausable
	 */
	public boolean untilHasMessage(long timeoutMillis) throws Pausable {
		final Task t = Task.getCurrentTask();
		boolean has_msg = hasMessage(t);
		long end = System.currentTimeMillis() + timeoutMillis;
		int iteration = 0;
		while (has_msg == false) {
			if (++iteration <= 3) {
				// Timer creation is costly, and frequently it has to
				// be cancelled anyway.  Try to avoid it:
				Task.yield();
			} else {
				TimerTask tt = startConsumerTimeoutTimer(t, timeoutMillis);
				Task.pause(this);
				tt.cancel();
			}
			has_msg = hasMessage(t);
			timeoutMillis = end - System.currentTimeMillis();
			if (timeoutMillis <= 0) {
				removeMessageConsumer(t);
				break;
			}
		}
		return has_msg;
	}

	/**
	 * @return non-null message.
	 * @throws Pausable
	 */
	public boolean untilHasMessages(int num, long timeoutMillis)
			throws Pausable {
		final Task t = Task.getCurrentTask();
		final long end = System.currentTimeMillis() + timeoutMillis;

		boolean has_msg = hasMessages(num, t);
		int iteration = 0;
		while (has_msg == false) {
			if (++iteration <= 3) {
				// Timer creation is costly, and frequently it has to
				// be cancelled anyway.  Try to avoid it:
				Task.yield();
			} else {
				TimerTask tt = startConsumerTimeoutTimer(t, timeoutMillis);
				Task.pause(this);
				if (!tt.cancel()) {
					removeMessageConsumer(t);
				}
			}

			has_msg = hasMessages(num, t);
			timeoutMillis = end - System.currentTimeMillis();
			if (!has_msg && timeoutMillis <= 0) {
				removeMessageConsumer(t);
				break;
			}
		}
		return has_msg;
	}

	private TimerTask startConsumerTimeoutTimer(final Task t, long timeoutMillis) {
		TimerTask tt = new TimerTask() {
			public void run() {
				if (removeMessageConsumer(t)) {
					t.consumeTimeout(Mailbox.this);
				}
			}
		};
		Task.timer.schedule(tt, timeoutMillis);
		return tt;
	}

	/**
	 * Non-blocking, nonpausing "wait-until-message-available".
	 * 
	 * @param eo
	 *            . If non-null, registers this observer and calls it with a
	 *            MessageAvailable event when a put() is done.
	 * @return true's one, or false
	 */
	public boolean hasMessage(MessageConsumer eo) {
		boolean has_msg;
		synchronized (this) {
			int n = numMsgs;
			if (n > 0) {
				has_msg = true;
			} else {
				has_msg = false;
				addMessageConsumer(eo);
			}
		}
		return has_msg;
	}

	public boolean hasMessages(int num, MessageConsumer eo) {
		boolean has_msg;
		synchronized (this) {
			int n = numMsgs;
			if (n >= num) {
				has_msg = true;
			} else {
				has_msg = false;
				addMessageConsumer(eo);
			}
		}
		return has_msg;
	}

	/**
	 * Non-blocking, nonpausing peek.
	 * 
	 * @return buffered message if there's one, or null
	 */
	public T peek(int idx) {
		assert idx >= 0 : "negative index";
		T msg;
		synchronized (this) {
			int n = numMsgs;
			if (idx < n) {
				int ic = icons;
				msg = msgs[(ic + idx) % msgs.length];
				
				assert msg != null : "peeked null message!";
			} else {
				msg = null;
			}
		}
		return msg;
	}

	public T remove(final int idx) {
		assert idx >= 0 : "negative index";
		T msg;
		synchronized (this) {
			int n = numMsgs;
			assert idx < numMsgs;
			if (idx < n) {
				int ic = icons;
				int mlen = msgs.length;
				msg = msgs[(ic + idx) % mlen];
				for (int i = idx; i > 0; i--) {
					msgs[(ic + i) % mlen] = msgs[(ic + i - 1) % mlen];
				}
				msgs[icons] = null;
				numMsgs -= 1;
				icons = (icons + 1) % mlen;
			} else {
				throw new IllegalStateException();
			}
		}
		return msg;
	}

	/**
	 * Non-blocking, nonpausing put.
	 * 
	 * @param eo
	 *            . If non-null, registers this observer and calls it with an
	 *            SpaceAvailable event when there's space.
	 * @return buffered message if there's one, or null
	 */
	public boolean put(T msg, MessageProducer eo) {
		boolean ret = true; // assume we will be able to enqueue
		MessageConsumer consumer;
		synchronized (this) {
			if (msg == null) {
				throw new NullPointerException("Null message supplied to put");
			}
			int ip = iprod;
			int ic = icons;
			int n = numMsgs;
			if (n == msgs.length) {
				assert ic == ip : "numElements == msgs.length && ic != ip";
				if (n < maxMsgs) {
					T[] newmsgs = (T[]) new Object[Math.min(n * 2, maxMsgs)];
					System.arraycopy(msgs, ic, newmsgs, 0, n - ic);
					if (ic > 0) {
						System.arraycopy(msgs, 0, newmsgs, n - ic, ic);
					}
					msgs = newmsgs;
					ip = n;
					ic = 0;
				} else {
					ret = false;
				}
			}
			if (ret) {
				numMsgs = n + 1;
				msgs[ip] = msg;
				iprod = (ip + 1) % msgs.length;
				icons = ic;
				consumer = sink;
				sink = null;
			} else {
				consumer = null;
				// unable to enqueue
				if (eo != null) {
					pendingProducers.add(eo);
				}
			}
		}
		// notify get's subscriber that something is available
		if (consumer != null) {
			consumer.messageAvailable(this);
		}
		return ret;
	}

	/**
	 * Get, don't pause or block.
	 * 
	 * @return stored message, or null if no message found.
	 */
	public T getnb() {
		return get(null);
	}

	/**
	 * @return non-null message.
	 * @throws Pausable
	 */
	public T get() throws Pausable {
		Task t = Task.getCurrentTask();
		T msg = get(t);
		while (msg == null) {
			Task.pause(this);
			removeMessageConsumer(t);
			msg = get(t);
		}
		return msg;
	}

	/**
	 * @return non-null message.
	 * @throws Pausable
	 */
	public T get(long timeoutMillis) throws Pausable {
		final Task t = Task.getCurrentTask();
		T msg = get(t);
		long end = System.currentTimeMillis() + timeoutMillis;
		while (msg == null) {
			TimerTask tt = new TimerTask() {
				public void run() {
					if (Mailbox.this.removeMessageConsumer(t)) {
						t.consumeTimeout(Mailbox.this);
					}
				}
			};
			Task.timer.schedule(tt, timeoutMillis);
			Task.pause(this);
			tt.cancel();
			removeMessageConsumer(t);
			msg = get(t);

			timeoutMillis = end - System.currentTimeMillis();
			if (timeoutMillis <= 0) {
				removeMessageConsumer(t);
				break;
			}
		}
		return msg;
	}

	public synchronized void addMessageProducer(MessageProducer spcSub) {
		pendingProducers.add(spcSub);
	}

	public synchronized boolean removeMessageProducer(MessageProducer spcSub) {
		return pendingProducers.remove(spcSub);
	}

	public synchronized void addMessageConsumer(MessageConsumer msgSub) {
		if (sink != null && sink != msgSub) {
			throw new AssertionError(
					"Error: A mailbox can not be shared by two consumers.  New = "
							+ msgSub + ", Old = " + sink);
		}
		sink = msgSub;
	}

	public synchronized boolean removeMessageConsumer(
			MessageConsumer msgSub) {
		if (sink == msgSub) {
			sink = null;
			return true;
		} else {
			return false;
		}
	}

	public boolean putnb(T msg) {
		return put(msg, null);
	}

	public void put(T msg) throws Pausable {
		Task t = Task.getCurrentTask();
		t.checkKill();
		while (!put(msg, t)) {
			Task.pause(this);
		}
	}

	public boolean put(T msg, int timeoutMillis) throws Pausable {
		final Task t = Task.getCurrentTask();
		long begin = System.currentTimeMillis();
		while (!put(msg, t)) {
			TimerTask tt = new TimerTask() {
				public void run() {
					if (Mailbox.this.removeMessageProducer(t)) {
						t.produceTimeout(Mailbox.this);
					}
				}
			};
			Task.timer.schedule(tt, timeoutMillis);
			Task.pause(this);
			tt.cancel();
			removeMessageProducer(t);
			if (System.currentTimeMillis() - begin >= timeoutMillis) {
				return false;
			}
		}
		return true;
	}

	public boolean putb(T msg) {
		return putb(msg, 0 /* infinite wait */);
	}

	public class BlockingSubscriber implements MessageProducer, MessageConsumer {
		public synchronized void spaceAvailable(Mailbox ep) {
			eventRcvd = true;
			notifyAll();
		}

		public synchronized void produceTimeout(Mailbox pub) {
			notifyAll();
		}

		public synchronized void messageAvailable(Mailbox ep) {
			eventRcvd = true;
			notifyAll();
		}

		public synchronized void consumeTimeout(Mailbox pub) {
			notifyAll();
		}

		public volatile boolean eventRcvd = false;

		public boolean blockingWait(final long timeoutMillis) {
			long absEnd = timeoutMillis == 0 
				? Long.MAX_VALUE 
				: System.currentTimeMillis() + timeoutMillis;
			synchronized (this) {
				long now = System.currentTimeMillis();
				while (!eventRcvd && (now < absEnd)) {
					try {
						this.wait(absEnd - now);
					} catch (InterruptedException ie) {
					}
					now = System.currentTimeMillis();
				}
			}
			
			return eventRcvd;
		}
	}

	public boolean putb(T msg, final long timeoutMillis) {
		BlockingSubscriber evs = new BlockingSubscriber();
		if (!put(msg, evs)) {
			boolean did_put = evs.blockingWait(timeoutMillis);
			removeMessageProducer(evs);
			return did_put;
		} else {
			return true;
		}
	}

	public synchronized int size() {
		return numMsgs;
	}

	public synchronized boolean hasMessage() {
		return numMsgs > 0;
	}

	/** return true if the mailbox has at least <code>num</code> messages. */
	public synchronized boolean hasMessages(int num) {
		return numMsgs >= num;
	}

	public synchronized boolean hasSpace() {
		return (maxMsgs - numMsgs) > 0;
	}

	/**
	 * retrieve a message, blocking the thread indefinitely. Note, this is a
	 * heavyweight block, unlike #get() that pauses the Fiber but doesn't block
	 * the thread.
	 */

	public T getb() {
		return getb(0);
	}

	/**
	 * retrieve a msg, and block the Java thread for the time given.
	 * 
	 * @param millis
	 *            . max wait time
	 * @return null if timed out.
	 */
	public T getb(final long timeoutMillis) {
		BlockingSubscriber evs = new BlockingSubscriber();
		T msg;

		if ((msg = get(evs)) == null) {
			if (evs.blockingWait(timeoutMillis)) {
				msg = get(null); // non-blocking get.
				assert msg != null : "Received event, but message is null";
			}
		}
		removeMessageConsumer(evs);
		return msg;
	}

	public synchronized String toString() {
		return "id:" + System.identityHashCode(this) + " " +
		// DEBUG "nGet:" + nGet + " " +
				// "nPut:" + nPut + " " +
				// "numWastedPuts:" + nWastedPuts + " " +
				// "nWastedGets:" + nWastedGets + " " +
				"numMsgs:" + numMsgs;
	}

	// Implementation of PauseReason
	public boolean isValid(Task t) {
		synchronized (this) {
			return (t == sink) || pendingProducers.contains(t);
		}
	}

	public synchronized Object[] messages() {
		synchronized (this) {
			Object[] result = new Object[numMsgs];
			for (int i = 0; i < numMsgs; i++) {
				result[i] = msgs[(icons + i) % msgs.length];
			}
			return result;
		}

	}
}
