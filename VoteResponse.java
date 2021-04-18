
public class VoteResponse {
	int requesterId;
	int responderId;
	boolean success;
	int term;
	public VoteResponse(int requesterId, int responderId, boolean success, int term) {
		this.requesterId = requesterId;
		this.responderId = responderId;
		this.success = success;
		this.term = term;
	}
}
