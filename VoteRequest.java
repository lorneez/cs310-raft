
public class VoteRequest {
	int senderId;
	int receiverId;
	int lastLogIndex;
	int lastLogTerm;
	public VoteRequest(int senderId, int receiverId, int lastLogIndex, int lastLogTerm) {
		this.senderId = senderId;
		this.receiverId = receiverId;
		this.lastLogIndex = lastLogIndex;
		this.lastLogTerm = lastLogTerm;
	}

    public int getReceiver() {
        return this.receiverId;
    }

    public int getLastLogIndex() {
        return this.lastLogIndex;
    }

    public int getLastLogTerm() {
        return this.lastLogTerm;
    }
}
