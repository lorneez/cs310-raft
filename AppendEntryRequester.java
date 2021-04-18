import java.util.ArrayList;

public class AppendEntryRequester {
	private ArrayList<ArrayList<AppendRequest>> mAppendRequestHistory;
	private ArrayList<ArrayList<AppendResponse>> mAppendResponseHistory;

	public AppendEntryRequester() {
		mAppendRequestHistory = new ArrayList<ArrayList<AppendRequest>>();
		mAppendResponseHistory = new ArrayList<ArrayList<AppendResponse>>();
	}

	public final void send (final int serverID,
		    final int leaderTerm,
		    final int leaderID,
		    final int prevLogIndex,
		    final int prevLogTerm,
		    final Entry[] entries,
		    final int leaderCommit) {
		AppendRequest appendRequest = new AppendRequest(leaderID, serverID, prevLogIndex, prevLogTerm, entries, leaderCommit);
		
		if (mAppendRequestHistory.size() < leaderTerm) {
			while(mAppendRequestHistory.size() < leaderTerm) {
				mAppendRequestHistory.add(new ArrayList<AppendRequest>());
			}
		}
		mAppendRequestHistory.get(leaderTerm - 1).add(appendRequest);
	}
	
	//students should call this class to check the vote responses
	public ArrayList<AppendResponse> getResponses(int term) {
		return mAppendResponseHistory.get(term - 1);
	}
	
	/**
	 * 
	 * All following methods are for testing purposes only
	 *
	 */
	public ArrayList<AppendRequest> getAppendRequestsForTerm(int term) {
		return mAppendRequestHistory.get(term - 1);
	}
	
	public void addAppendResponseForTerm(int term, AppendResponse appendResponse) {
		if (mAppendResponseHistory.size() < term) {
			while(mAppendResponseHistory.size() < term) {
				mAppendResponseHistory.add(new ArrayList<AppendResponse>());
			}
		}
		mAppendResponseHistory.get(term - 1).add(appendResponse);
	}
	
	public void clearResponsesForTerm(int term) {
		mAppendResponseHistory.get(term - 1).clear();
	}
	
	public void clearRequestsForTerm(int term) {
		mAppendRequestHistory.get(term - 1).clear();
	}
	
}
