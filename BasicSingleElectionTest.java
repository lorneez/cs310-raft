public class BasicSingleElectionTest {
	public static void main(String[] args) {
        System.out.println("@ag350:test:start");

        int ExpectedAssertions = 17;
        AssertionCount assertionCount = new AssertionCount();

        try {
            int A_ID = 0;
	    		int B_ID = 1;
	    		int C_ID = 2;

	    		int numServers = 3;

	    		RaftLog logA = new RaftLog(new Entry[] { });
	    		RaftLog logB = new RaftLog(new Entry[] { });
	    		RaftLog logC = new RaftLog(new Entry[] { });

	    		Timer timerA = new Timer();
	    		Timer timerB = new Timer();
	    		Timer timerC = new Timer();

	    		VoteRequester voteRequesterA = new VoteRequester();
	    		VoteRequester voteRequesterB = new VoteRequester();
	    		VoteRequester voteRequesterC = new VoteRequester();

	    		RaftNode serverA = new RaftNode(logA, timerA, new AppendEntryRequester(), voteRequesterA, numServers, A_ID, RaftMode.FOLLOWER, 1);
	    		RaftNode serverB = new RaftNode(logB, timerB, new AppendEntryRequester(), voteRequesterB, numServers, B_ID, RaftMode.FOLLOWER, 1);
	    		RaftNode serverC = new RaftNode(logC, timerC, new AppendEntryRequester(), voteRequesterC, numServers, C_ID, RaftMode.FOLLOWER, 1);

	    		PragmaProducer.ExpectMode("Server A starts off as follower", "A", serverA, RaftMode.FOLLOWER, assertionCount);
	        PragmaProducer.ExpectMode("Server B starts off as follower", "B", serverB, RaftMode.FOLLOWER, assertionCount);
	        PragmaProducer.ExpectMode("Server C starts off as follower", "C", serverC, RaftMode.FOLLOWER, assertionCount);


	    		serverA.handleTimeout();
	        // Server A should become candidate after its first timeout
	        PragmaProducer.ExpectMode("Server A becomes candidate in term 2", "A", serverA, RaftMode.CANDIDATE, assertionCount);
	        PragmaProducer.ExpectMode("Server B remains follower in term 2", "B", serverB, RaftMode.FOLLOWER, assertionCount);
	        PragmaProducer.ExpectMode("Server C remains follower in term 2", "C", serverC, RaftMode.FOLLOWER, assertionCount);

	        // Ensure Server A is now in term 2
	        PragmaProducer.ExpectTerm("Server A moves to term 2 after becoming candidate", "A", serverA, 2, assertionCount);

	        // Check that all of the servers have received the correct vote requests
	    		// This includes term, id, last log index, and last log term
	        PragmaProducer.ExpectVoteRequests("Server A sent vote requests to B and C", "A", voteRequesterA, 2, new int[] { B_ID, C_ID }, -1, -1, assertionCount);


	        int term_2 = 2;

	        // Send vote requests to Server B and C
	    		int serverBResponse = serverB.receiveVoteRequest(term_2, A_ID, 1, 1);
	    		int serverCResponse = serverC.receiveVoteRequest(term_2, A_ID, 1, 1);

	        // Server B and C should vote for Server A
	        PragmaProducer.ExpectVoteResponse("Server B votes for A", "B", serverBResponse, 0, assertionCount);
	        PragmaProducer.ExpectVoteResponse("Server C votes for A", "C", serverCResponse, 0, assertionCount);

	        PragmaProducer.ExpectMode("Server B still follower after voting for A", "B", serverB, RaftMode.FOLLOWER, assertionCount);
	        PragmaProducer.ExpectMode("Server C still follower after voting for A", "C", serverC, RaftMode.FOLLOWER, assertionCount);


	        // Inject responses to Server A so it can properly become leader on next timeout
	        // Server A gets votes from itself and Server B and C
	    		voteRequesterA.addVoteResponseForTerm(2, new VoteResponse(A_ID, A_ID, true, term_2));
	    		voteRequesterA.addVoteResponseForTerm(2, new VoteResponse(A_ID, B_ID, true, term_2));
	    		voteRequesterA.addVoteResponseForTerm(2, new VoteResponse(A_ID, C_ID, true, term_2));

	    		serverA.handleTimeout();
	        // Server A should become leader after timeout
	        PragmaProducer.ExpectMode("Server A elected leader in term 2", "A", serverA, RaftMode.LEADER, assertionCount);
	        PragmaProducer.ExpectMode("Server B still follower at end of term 2", "B", serverB, RaftMode.FOLLOWER, assertionCount);
	        PragmaProducer.ExpectMode("Server C still be follower at end of term 2", "C", serverC, RaftMode.FOLLOWER, assertionCount);

	        PragmaProducer.ExpectTerm("Server A has correct term as leader", "A", serverA, 2, assertionCount);


            System.out.println("@ag350:assertion:start");
            String assertion = String.format("@ag350:assertion:data { \"name\": \"Protocol\", \"status\": \"PASS\", \"data\": { } }");
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");

	        // TODO: ensure heartbeats reset timer

	        System.out.println("@ag350:test:end");
        } catch (Exception e) {
            System.out.println("@ag350:assertion:start");
            String assertion = String.format("@ag350:assertion:data { \"name\": \"Protocol\", \"status\": \"FAIL\", \"data\": { \"reason:\": \"%s\" } }", e.getMessage());
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");

            int printedAssertions = assertionCount.getCount();
            PragmaProducer.PrintDidNotReachAssertion(ExpectedAssertions - printedAssertions - 1);

            System.out.println("@ag350:test:end");
        }

	}
}
