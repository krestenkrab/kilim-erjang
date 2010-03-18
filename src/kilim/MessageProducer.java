package kilim;

public interface MessageProducer {
	void spaceAvailable(Mailbox src);
	void produceTimeout(Mailbox pub);
}
