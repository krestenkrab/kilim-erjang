package kilim;

public interface MessageConsumer {
	void messageAvailable(Mailbox pub);
	void consumeTimeout(Mailbox pub);
}
