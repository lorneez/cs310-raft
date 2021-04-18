import java.util.ArrayList;

public class VoteRequester {
    private ArrayList<ArrayList<VoteRequest>> mVoteRequestHistory;
    private ArrayList<ArrayList<VoteResponse>> mVoteResponseHistory;

    public VoteRequester() {
        mVoteRequestHistory = new ArrayList<ArrayList<VoteRequest>>();
        mVoteResponseHistory = new ArrayList<ArrayList<VoteResponse>>();
    }

	/**
	 * Send a RequestVote RPC to serverID.
	 * @param serverID the peer to send request to
	 * @param candidateTerm the term for this request
	 * @param candidateID the sender of this request (the candidate)
	 * @param lastLogIndex the last index in candidate's log
	 * @param lastLogTerm the latest term in the candidate's log
	 */

    public void send(final int serverID,
                     final int candidateTerm,
                     final int candidateID,
                     final int lastLogIndex,
                     final int lastLogTerm) {
        // We could just verify that they are correctly setting their own candidate id
        VoteRequest voteRequest = new VoteRequest(candidateID, serverID, lastLogIndex, lastLogTerm);
        if (mVoteRequestHistory.size() < candidateTerm) {
            while (mVoteRequestHistory.size() < candidateTerm) {
                mVoteRequestHistory.add(new ArrayList<VoteRequest>());
            }
        }
        mVoteRequestHistory.get(candidateTerm - 1).add(voteRequest);
    }

	/**
	 * Methods to check responses for RequestVote RPCs sent from this server in specified term.
	 * getResponses, countYesResponses, maxResponseTerm
	 */

	public ArrayList<VoteResponse> getResponses(int term) {
	    fillToTerm(term);
	    return mVoteResponseHistory.get(term - 1);
    }

	public int countYesResponses(int term) {
		ArrayList<VoteResponse> responses = getResponses(term);
		int yesResponses = 1;
		for (int i = 0; i < responses.size(); i++) {
			if (responses.get(i).success) {
				yesResponses++;
			}
		}
		return yesResponses;
    }

	public int maxResponseTerm(int term) {
		ArrayList<VoteResponse> responses = getResponses(term);
		int maxReturnedTerm = 0;
		for (int i = 0; i < responses.size(); i++) {
			maxReturnedTerm = Math.max(responses.get(i).term, maxReturnedTerm);
		}
		return maxReturnedTerm;
	}

    /**
     * The following methods are called by test harness only
     */
    public ArrayList<VoteRequest> getVoteRequestsForTerm(int term) throws Exception {
        try {
            return mVoteRequestHistory.get(term - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new Exception("Attempted to get vote requests for non-existent term: " + term);
        }
    }

    private void fillToTerm(int term) {
        if (mVoteResponseHistory.size() < term) {
            while (mVoteResponseHistory.size() < term) {
                mVoteResponseHistory.add(new ArrayList<VoteResponse>());
            }
        }
    }

    public void addVoteResponseForTerm(int term, VoteResponse voteResponse) {
        fillToTerm(term);
        mVoteResponseHistory.get(term - 1).add(voteResponse);
    }

    public void clearResponsesForTerm(int term) {
        mVoteResponseHistory.get(term - 1).clear();
    }
}
