import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class RaftNode {

	private RaftLog mLog;
	private Timer mTimer;
	private AppendEntryRequester mAppendEntryRequester;
	private VoteRequester mVoteRequester;
	private int mNumServers;
	private int mNodeServerNumber;
	private RaftMode mCurrentMode;
	private int mTerm;
	private int mCommitIndex;
	private int[] nextIndex; // COMMENT
	private int[] matchIndex; // COMMENT
	private Set<Integer> voteTerms = new HashSet<>(); // COMMENT

	/**
	 * RaftNode constructor, called by test harness.
	 * @param log this replica's local log, pre-populated with entries
	 * @param timer this replica's local timer
	 * @param sendAppendEntry call this to send RPCs as leader and check responses
	 * @param sendVoteRequest call this to send RPCs as candidate and check responses
	 * @param numServers how many servers in the configuration (numbered starting at 0)
	 * @param nodeServerNumber server number for this replica (this RaftNode object)
	 * @param currentMode initial mode for this replica
	 * @param term initial term (e.g., last term seen before failure).  Terms start at 1.
	 **/
	public RaftNode(
			RaftLog log,
			Timer timer,
			AppendEntryRequester sendAppendEntry,
			VoteRequester sendVoteRequest,
			int numServers,
			int nodeServerNumber,
			RaftMode currentMode,
			int term) {
		mLog = log;
		mTimer = timer;
		mAppendEntryRequester = sendAppendEntry;
		mVoteRequester = sendVoteRequest;
		mNumServers = numServers;
		mNodeServerNumber = nodeServerNumber;
		mCurrentMode = currentMode;
		mTerm = term;
		mCommitIndex = -1;
	}

	public RaftMode getCurrentMode() {
		return mCurrentMode;
	}

	public int getCommitIndex() {
		return mCommitIndex;
	}

	public int getTerm() {
		return mTerm;
	}

	public RaftLog getCurrentLog() {
		return mLog;
	}

	public int getServerId() {
		return mNodeServerNumber;
	}

	public int getLastApplied() {
		return mLog.getLastEntryIndex();
	}

	/**
	 * Returns whether or not the server has already voted in this term.
	 * @param term term to check
	 * @return boolean
	 */
	public boolean votedInTerm(int term) {
	   	 return voteTerms.contains(term);
	}

	/**
	 * @param candidateTerm candidate's term
	 * @param candidateID   candidate requesting vote
	 * @param lastLogIndex  index of candidate's last log entry
	 * @param lastLogTerm   term of candidate's last log entry
	 * @return 0, if server votes for candidate; otherwise, server's current term
	 */
	public int receiveVoteRequest(int candidateTerm,
								  int candidateID,
								  int lastLogIndex,
								  int lastLogTerm) {

		if(mCurrentMode != RaftMode.FOLLOWER) {
			if(candidateTerm > mTerm) {
				mCurrentMode = RaftMode.FOLLOWER;
				mTerm = candidateTerm;
			}
		}
		if(candidateTerm < mTerm) {
			return mTerm;
		}
		if(votedInTerm(candidateTerm)) {
			return candidateTerm;
		}
		if(mLog.getLastEntryTerm() > lastLogTerm) {
			return candidateTerm;
		}
		mTerm = candidateTerm;
		voteTerms.add(mTerm);
		return 0;
	}

	/**
	 * @param leaderTerm   leader's term
	 * @param leaderID     current leader
	 * @param prevLogIndex index of log entry before entries to append
	 * @param prevLogTerm  term of log entry before entries to append
	 * @param entries      entries to append (in order of 0 to append.length-1)
	 * @param leaderCommit index of highest committed entry
	 * @return 0, if follower accepts; otherwise, follower's current term
	 */
	public int receiveAppendEntry(int leaderTerm,
								  int leaderID,
								  int prevLogIndex,
								  int prevLogTerm,
								  Entry[] entries,
								  int leaderCommit) {

		if(mCurrentMode != RaftMode.FOLLOWER) {
			if(leaderTerm >= mTerm) {
				mCurrentMode = RaftMode.FOLLOWER;
				mTerm = leaderTerm;
			}
		}
		if(leaderTerm < mTerm) {
			return mTerm;
		}
		if(prevLogIndex != -1) {
			if(mLog.getEntry(prevLogIndex) == null || mLog.getEntry(prevLogIndex).term != prevLogTerm) {
				return leaderTerm;
			}
		}
		mLog.insert(entries, prevLogIndex);
		if(leaderCommit > mCommitIndex) {
			mCommitIndex = Math.min(leaderCommit, mLog.getLastEntryIndex());
		}
		return 0;
	}

	/**
	 * Handle timeout
	 * Followers will become candidates and start an election.
	 * Candidates will check votes. If obtained majority of votes and still valid candidate, become leader.
	 * Leaders will perform log repair and send updated logs to each follower server.
	 * @return
	 */
	public int handleTimeout() {
		if(mCurrentMode == RaftMode.FOLLOWER) {
			startElection();
			return 0;
		}
		else if(mCurrentMode == RaftMode.CANDIDATE) {
			checkResponses();
			return 0;
		}
		else {
			ArrayList<AppendResponse> res = mAppendEntryRequester.getResponses(mTerm);
			int follower = 0;
			for(AppendResponse response : res) {
				int index = response.responderId;
				if(response.term > mTerm) {
					mCurrentMode = RaftMode.FOLLOWER;
					mTerm = response.term;
				}
				if(!res.get(follower).success) {
					nextIndex[follower] --;
					if(mLog.getLastEntryIndex() >= nextIndex[follower]) {
						int logSize = mLog.getLastEntryIndex();
						Entry[] logEntries;
						if(nextIndex[follower] < 0) {
							logEntries = new Entry[logSize];
							for(int j = 0; j < logSize; j++) {
								logEntries[j] = mLog.getEntry(j);
							}
							mAppendEntryRequester.send(
									index,
									mTerm,
									mNodeServerNumber,
									nextIndex[follower],
									-1,
									logEntries,
									mCommitIndex
							);
						}
						else {
							logEntries = new Entry[logSize - nextIndex[follower]];
							for(int j = 0; j < logSize - nextIndex[follower]; j++) {
								logEntries[j] = mLog.getEntry(j);
							}
							mAppendEntryRequester.send(
									index,
									mTerm,
									mNodeServerNumber,
									nextIndex[follower],
									mLog.getEntry(nextIndex[follower]).term,
									logEntries,
									mCommitIndex
							);
						}
					}
				}
				else {
					nextIndex[follower] = mLog.getLastEntryIndex() + 1;
					matchIndex[follower] = mLog.getLastEntryIndex() + 1;
				}
				// Should not be necessary
				for(int N=0; N<=mLog.getLastEntryIndex(); N++) {
					if(mLog.getEntry(N).term == mTerm && matchIndex[follower] > N && N > mCommitIndex) {
						mCommitIndex = N;
					}
				}
				follower ++;
			}
			sendHeartbeat();
			return 0;
		}
	}

	/**
	 * Method to print all the responses of follower servers
	 * @param res ArrayList of AppendResponse objects
	 */
	private void printResponses(ArrayList<AppendResponse> res) {
		for(AppendResponse i : res) {
			System.out.println(i.requesterId + " " + i.responderId + " " + i.success + " " + i.term);
		}
	}

	/**
	 *  Private method shows how to issue a round of RPC calls.
	 *  Responses come in over time: after at least one timer interval, call mVoteRequester to query/retrieve responses.
	 */
	private void requestVotes() {
		for (int i = 0; i < mNumServers; i++) {
			if (i != mNodeServerNumber) {
				mVoteRequester.send(i, mTerm, mNodeServerNumber, mLog.getLastEntryIndex(), mLog.getLastEntryTerm());
			}
		}
	}

	/**
	 * Begin the election by becoming a candidate, updating term, and requesting votes from the other servers.
	 */
	private void startElection() {
		mCurrentMode = RaftMode.CANDIDATE;
		mTerm ++;
		voteTerms.add(mTerm);
		requestVotes();
	}

	/**
	 * Method to check responses of followers. If candidate still valid, check if majority has been obtained.
	 * If majority obtained, become leader, else, start a new election.
	 */
	private void checkResponses() {
		int maxTerm = mVoteRequester.maxResponseTerm(mTerm);
		if(maxTerm > mTerm) {
			mCurrentMode = RaftMode.FOLLOWER;
			mTerm = maxTerm;
			return;
		}
		boolean hasMajority = mVoteRequester.countYesResponses(mTerm) > (mNumServers/2);
		if(hasMajority) {
			mCurrentMode = RaftMode.LEADER;
			nextIndex = new int[mNumServers];
			matchIndex = new int[mNumServers];
			for(int i=0; i<mNumServers; i++) {
				nextIndex[i] = mLog.getLastEntryIndex();
				matchIndex[i] = 0;
			}
			sendHeartbeat();
		}
		else {
			startElection();
		}
		return;
	}

	/**
	 * Send blank append entry requests to followers to let them know of leader existence.
	 */
	private void sendHeartbeat() {
		for (int i = 0; i < mNumServers; i++) {
			if (i != mNodeServerNumber) {
				mAppendEntryRequester.send(
						i,
						mTerm,
						mNodeServerNumber,
						mLog.getLastEntryIndex(),
						mLog.getLastEntryTerm(),
						new Entry[0],
						mCommitIndex
						);
			}
		}
	}
}

