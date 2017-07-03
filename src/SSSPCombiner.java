import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

public final class SSSPCombiner extends MessageCombiner<Long, Long> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void combineMessages(MessageIterator<Long> messages) {

        long minMessage = Long.MAX_VALUE;
        for (Long msg: messages) {
           minMessage = Math.min(minMessage, msg);
        }
        sendCombinedMessage(minMessage);
    }
}