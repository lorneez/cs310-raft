
public class AppendRequest {
	int leaderId;
	int receiverId;
	int prevLogIndex;
	int prevLogTerm;
	Entry[] entries;
	int leaderCommit;
	public AppendRequest(int leaderId, int receiverId, int prevLogIndex, int prevLogTerm, Entry[] entries, int leaderCommit) {
		this.leaderId = leaderId;
		this.receiverId = receiverId;
		this.prevLogIndex = prevLogIndex;
		this.prevLogTerm = prevLogTerm;
		this.entries = entries;
		this.leaderCommit = leaderCommit;
	}
	
	public int getReceiverId() {
		return receiverId;
	}
	
	public int getPrevLogIndex() {
		return prevLogIndex;
	}
	
	public int getPrevLogTerm() {
		return prevLogTerm;
	}
	
	public Entry[] getEntries() {
		return entries;
	}
	
	public int getLeaderCommit() {
		return leaderCommit;
	}
}