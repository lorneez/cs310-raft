import java.util.ArrayList;

public class PragmaProducer {
    public static String raftModeToString(RaftMode raftMode) {
		if (raftMode == RaftMode.FOLLOWER) {
			return "follower";
		}
		if (raftMode == RaftMode.CANDIDATE) {
			return "candidate";
		}
		if (raftMode == RaftMode.LEADER) {
			return "leader";
		}
		return "";
	}

    public static void ExpectVotedIn(String name, String serverName, RaftNode server, int term, AssertionCount assertionCount) throws Exception {
        System.out.println("@ag350:assertion:start");

        boolean votedInTerm = server.votedInTerm(term);
        if (votedInTerm) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"PASS\", \"data\": { \"term:\": \"%d\" } }", name, term);
            System.out.println(assertion);
        } else {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"term:\": \"%d\" } }", name, term);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " did not vote in term " + term);
        }

        System.out.println("@ag350:assertion:end");
        assertionCount.addAssertion();
    }

    public static void ExpectMode(String name, String serverName, RaftNode server, RaftMode expectedMode, AssertionCount assertionCount) throws Exception {
        System.out.println("@ag350:assertion:start");

        String expectedRaftModeString = raftModeToString(expectedMode);
        RaftMode raftMode = server.getCurrentMode();
        String raftModeString = raftModeToString(raftMode);

        if (raftMode == expectedMode) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"PASS\", \"data\": { \"expected:\": \"%s\", \"actual\": \"%s\" } }", name, expectedRaftModeString, raftModeString);
            System.out.println(assertion);
        } else {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"expected:\": \"%s\", \"actual\": \"%s\" } }", name, expectedRaftModeString, raftModeString);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " has incorrect mode");
        }

        System.out.println("@ag350:assertion:end");
        assertionCount.addAssertion();
    }

    public static void ExpectTerm(String name, String serverName, RaftNode server, int expectedTerm, AssertionCount assertionCount) throws Exception {
        System.out.println("@ag350:assertion:start");

        int term = server.getTerm();

        if (term == expectedTerm) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"PASS\", \"data\": { \"expected:\": \"%d\", \"actual\": \"%d\" } }", name, expectedTerm, term);
            System.out.println(assertion);
        } else {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"expected:\": \"%d\", \"actual\": \"%d\" } }", name, expectedTerm, term);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " has incorrect term");
        }

        System.out.println("@ag350:assertion:end");
        assertionCount.addAssertion();
    }

    public static void ExpectVoteResponse(String name, String serverName, int response, int expectedResponse, AssertionCount assertionCount) throws Exception {
        System.out.println("@ag350:assertion:start");

        if (response == expectedResponse) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"PASS\", \"data\": { \"expected:\": \"%d\", \"actual\": \"%d\" } }", name, expectedResponse, response);
            System.out.println(assertion);
        } else {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"expected:\": \"%d\", \"actual\": \"%d\" } }", name, expectedResponse, response);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " had incorrect vote response");
        }

        System.out.println("@ag350:assertion:end");
        assertionCount.addAssertion();
    }

    public static void ExpectVoteRequests(String name, String serverName, VoteRequester voteRequester, int term, int[] receivers, int expectedLastLogIndex, int expectedLastLogTerm, AssertionCount assertionCount) throws Exception {
        System.out.println("@ag350:assertion:start");

        ArrayList<VoteRequest> requests = voteRequester.getVoteRequestsForTerm(term);

        if (requests.size() != receivers.length) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"Incorrect number of vote requests sent\" } }", name);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " had incorrect votes requested");
        }

        for (int i = 0; i < receivers.length; i++) {
            VoteRequest r = findVoteRequest(requests, receivers[i]);
            if (r == null) {
                String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"Did not send vote request to desired recipient\" } }", name);
                System.out.println(assertion);
                System.out.println("@ag350:assertion:end");
                assertionCount.addAssertion();
                throw new Exception("Server " + serverName + " had incorrect votes requested");
            }

            if (r.getLastLogIndex() != expectedLastLogIndex) {
                String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"Requested vote with incorrect last log index\", \"lastLog\": %d, \"expectedLastLog\": %d } }", name, r.getLastLogIndex(), expectedLastLogIndex);
                System.out.println(assertion);
                System.out.println("@ag350:assertion:end");
                assertionCount.addAssertion();
                throw new Exception("Server " + serverName + " had incorrect votes requested");
            }

            if (r.getLastLogTerm() != expectedLastLogTerm) {
                String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"Requested vote with incorrect last log term\", \"lastTerm\": %d, \"expectedLastTerm\": %d } }", name, r.getLastLogTerm(), expectedLastLogTerm);
                System.out.println(assertion);
                System.out.println("@ag350:assertion:end");
                assertionCount.addAssertion();
                throw new Exception("Server " + serverName + " had incorrect votes requested");
            }
        }

        String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"PASS\", \"data\": { } }", name);
        System.out.println(assertion);
        System.out.println("@ag350:assertion:end");
        assertionCount.addAssertion();
    }

    public static void ExpectAppendEntryRequest(String name, String serverName, AppendEntryRequester appendEntryRequester, int term, int receiverId, int expectedPrevLogIndex, int expectedPrevLogTerm, int expectedLeaderCommit, AssertionCount assertionCount) throws Exception {
        System.out.println("@ag350:assertion:start");

        ArrayList<AppendRequest> requests = appendEntryRequester.getAppendRequestsForTerm(term);
        AppendRequest r = null;
        for (int i = 0; i < requests.size(); i++) {
        		if (requests.get(i).getReceiverId() == receiverId) {
        			r = requests.get(i);
        			break;
        		}
        }

        if (r == null) {
        		String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"Append entry request not sent to server %d\" } }", name, receiverId);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " had incorrect append entry requested");
        }

        if (r.getPrevLogIndex() != expectedPrevLogIndex) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"Append entry request with incorrect last log index\", \"prevLogIndex\": %d, \"expectedPrevLogIndex\": %d } }", name, r.getPrevLogIndex(), expectedPrevLogIndex);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " had incorrect append entry requested");
        }

        if (r.getPrevLogTerm() != expectedPrevLogTerm) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"Append entry request with incorrect previous log term\", \"prevLogTerm\": %d, \"expectedPrevLogTerm\": %d } }", name, r.getPrevLogTerm(), expectedPrevLogTerm);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " had incorrect append entry requested");
        }

        if (r.getLeaderCommit() != expectedLeaderCommit) {
        		String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"Append entry request with incorrect leader commit\", \"leaderCommit\": %d, \"expectedLeaderCommit\": %d } }", name, r.getLeaderCommit(), expectedLeaderCommit);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " had incorrect append entry requested");
        }

        String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"PASS\", \"data\": { } }", name);
        System.out.println(assertion);
        System.out.println("@ag350:assertion:end");
        assertionCount.addAssertion();
    }

    public static void ExpectAppendEntryResponse(String name, String serverName, int response, int expectedResponse, AssertionCount assertionCount) throws Exception {
        System.out.println("@ag350:assertion:start");

        if (response == expectedResponse) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"PASS\", \"data\": { \"expected:\": \"%d\", \"actual\": \"%d\" } }", name, expectedResponse, response);
            System.out.println(assertion);
        } else {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"expected:\": \"%d\", \"actual\": \"%d\" } }", name, expectedResponse, response);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " had incorrect vote response");
        }

        System.out.println("@ag350:assertion:end");
        assertionCount.addAssertion();
    }

    public static void ExpectCommitIndex(String name, String serverName, int commitIndex, int expectedCommitIndex, AssertionCount assertionCount) throws Exception {
        System.out.println("@ag350:assertion:start");

        if (commitIndex == expectedCommitIndex) {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"PASS\", \"data\": { \"expected:\": \"%d\", \"actual\": \"%d\" } }", name, expectedCommitIndex, commitIndex);
            System.out.println(assertion);
        } else {
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { \"expected:\": \"%d\", \"actual\": \"%d\" } }", name, expectedCommitIndex, commitIndex);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
            assertionCount.addAssertion();
            throw new Exception("Server " + serverName + " had incorrect commit index");
        }

        System.out.println("@ag350:assertion:end");
        assertionCount.addAssertion();
    }

    public static void PrintDidNotReachAssertion(int times) {
        for (int i = 0; i < times; i++) {
            String name = "Did not reach assertion " + (i + 1);
            System.out.println("@ag350:assertion:start");
            String assertion = String.format("@ag350:assertion:data { \"name\": \"%s\", \"status\": \"FAIL\", \"data\": { } }", name);
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");
        }
    }

    private static VoteRequest findVoteRequest(ArrayList<VoteRequest> requests, int receiver) {
        for (int i = 0; i < requests.size(); i++) {
            if (requests.get(i).getReceiver() == receiver) {
                return requests.get(i);
            }
        }

        return null;
    }
}
