
public class AppendEntriesTest {

	public static void main(String[] args) {
        System.out.println("@ag350:test:start");

        int ExpectedAssertions = 55;
        AssertionCount assertionCount = new AssertionCount();

        try {
            int A_ID = 0;
	    		int B_ID = 1;
	    		int C_ID = 2;
	    		int D_ID = 3;
	    		int E_ID = 4;

	    		int numServers = 5;

	    		RaftLog logA = new RaftLog(new Entry[] { new Entry(1, 1), new Entry(1, 2) });
	    		RaftLog logB = new RaftLog(new Entry[] { new Entry(1, 1) });
	    		RaftLog logC = new RaftLog(new Entry[] {});
	    		RaftLog logD = new RaftLog(new Entry[] {});
	    		RaftLog logE = new RaftLog(new Entry[] { new Entry(1, 4) });
				System.out.println("TEST: (initialized all logs) Success ");

	    		Timer timerA = new Timer();
	    		Timer timerB = new Timer();
	    		Timer timerC = new Timer();

	    		VoteRequester voteRequesterA = new VoteRequester();
	    		VoteRequester voteRequesterB = new VoteRequester();
	    		VoteRequester voteRequesterC = new VoteRequester();
	    		VoteRequester voteRequesterD = new VoteRequester();
	    		VoteRequester voteRequesterE = new VoteRequester();

	    		AppendEntryRequester appendEntryRequesterA = new AppendEntryRequester();

	    		RaftNode serverA = new RaftNode(logA, timerA, appendEntryRequesterA, voteRequesterA, numServers, A_ID, RaftMode.FOLLOWER, 5);
	    		RaftNode serverB = new RaftNode(logB, timerB, new AppendEntryRequester(), voteRequesterB, numServers, B_ID, RaftMode.FOLLOWER, 5);
	    		RaftNode serverC = new RaftNode(logC, timerC, new AppendEntryRequester(), voteRequesterC, numServers, C_ID, RaftMode.FOLLOWER, 5);
	    		RaftNode serverD = new RaftNode(logD, timerB, new AppendEntryRequester(), voteRequesterD, numServers, D_ID, RaftMode.FOLLOWER, 5);
	    		RaftNode serverE = new RaftNode(logE, timerC, new AppendEntryRequester(), voteRequesterE, numServers, E_ID, RaftMode.FOLLOWER, 5);
	    		PragmaProducer.ExpectMode("Server A starts off as follower", "A", serverA, RaftMode.FOLLOWER, assertionCount);
	    		PragmaProducer.ExpectMode("Server B starts off as follower", "B", serverB, RaftMode.FOLLOWER, assertionCount);
            	PragmaProducer.ExpectMode("Server C starts off as follower", "C", serverC, RaftMode.FOLLOWER, assertionCount);
            	PragmaProducer.ExpectMode("Server D starts off as follower", "D", serverD, RaftMode.FOLLOWER, assertionCount);
            	PragmaProducer.ExpectMode("Server E starts off as follower", "E", serverE, RaftMode.FOLLOWER, assertionCount);

            	System.out.println("TEST: (all servers are followers) Success ");

            	// A becomes candidate
	    		serverA.handleTimeout();

	    		PragmaProducer.ExpectMode("Server A becomes candidate in term 6", "A", serverA, RaftMode.CANDIDATE, assertionCount);

				System.out.println("TEST: (A becomes candidate) Success ");

	    		// All other servers receive vote requests
	    		int bResponse = serverB.receiveVoteRequest(6, A_ID, 1, 2);
	    		int cResponse = serverC.receiveVoteRequest(6, A_ID, 1, 2);
	    		int dResponse = serverD.receiveVoteRequest(6, A_ID, 1, 2);
	    		int eResponse = serverE.receiveVoteRequest(6, A_ID, 1, 2);

	    	    PragmaProducer.ExpectVoteResponse("Server B votes for A", "B", bResponse, 0, assertionCount);
            	PragmaProducer.ExpectVoteResponse("Server C votes for A", "C", cResponse, 0, assertionCount);
            	PragmaProducer.ExpectVoteResponse("Server D votes for A", "D", dResponse, 0, assertionCount);
            	PragmaProducer.ExpectVoteResponse("Server E does not vote for A", "E", eResponse, 6, assertionCount);

				System.out.println("TEST: (all but E vote for A) Success ");

            	// All servers should vote for A except for E
            	PragmaProducer.ExpectMode("Server B still follower after voting for A", "B", serverB, RaftMode.FOLLOWER, assertionCount);
            	PragmaProducer.ExpectMode("Server C still follower after voting for A", "C", serverC, RaftMode.FOLLOWER, assertionCount);
            	PragmaProducer.ExpectMode("Server D still follower after voting for A", "D", serverD, RaftMode.FOLLOWER, assertionCount);
            	PragmaProducer.ExpectMode("Server E still follower after not voting for A", "E", serverE, RaftMode.FOLLOWER, assertionCount);

				System.out.println("TEST: (all servers follow A) Success ");

	    		voteRequesterA.addVoteResponseForTerm(6, new VoteResponse(0, 1, true, -1));
	    		voteRequesterA.addVoteResponseForTerm(6, new VoteResponse(0, 2, true, -1));
	    		voteRequesterA.addVoteResponseForTerm(6, new VoteResponse(0, 3, true, -1));
	    		voteRequesterA.addVoteResponseForTerm(6, new VoteResponse(0, 4, false, 6));

	    		// Server A is now leader
	    		serverA.handleTimeout();
				System.out.println("TEST: (A becomes leader) Success ");

	    		//Check that A sends out all the heartbeats
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to B", "A", appendEntryRequesterA, 6, 1, 1, 2, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to C", "A", appendEntryRequesterA, 6, 2, 1, 2, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to D", "A", appendEntryRequesterA, 6, 3, 1, 2, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to E", "A", appendEntryRequesterA, 6, 4, 1, 2, -1, assertionCount);

				System.out.println("TEST: (A sends out heartbeat) Success ");

	    		// Append Entry requests successfully get to all other servers
	    		bResponse = serverB.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, -1);
	    		cResponse = serverC.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, -1);
	    		dResponse = serverD.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, -1);
	    		eResponse = serverE.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, -1);

	    		PragmaProducer.ExpectAppendEntryResponse("Server B rejects A's append entry request", "B", bResponse, 6, assertionCount);
	    		PragmaProducer.ExpectAppendEntryResponse("Server C rejects A's append entry request", "C", cResponse, 6, assertionCount);
	    		PragmaProducer.ExpectAppendEntryResponse("Server D rejects A's append entry request", "D", dResponse, 6, assertionCount);
	    		PragmaProducer.ExpectAppendEntryResponse("Server E rejects A's append entry request", "E", eResponse, 6, assertionCount);

	    		System.out.println("TEST: (all servers reject A) Success ");

	    		// All servers respond to A, and their requests get through. They all reject.
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 1, false, 6));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 2, false, 6));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 3, false, 6));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 4, false, 6));

				System.out.println("TEST: (all servers send rejection) Success ");

	    		// Necessary cleanup before handleTimeout is called.
	    		appendEntryRequesterA.clearRequestsForTerm(6);
	    		serverA.handleTimeout();

				System.out.println("TEST: (A obtains objections) Success ");

	    		// Check that a dropped index down by 1, and resent requests to all servers
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to B", "A", appendEntryRequesterA, 6, 1, 0, 1, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to C", "A", appendEntryRequesterA, 6, 2, 0, 1, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to D", "A", appendEntryRequesterA, 6, 3, 0, 1, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to E", "A", appendEntryRequesterA, 6, 4, 0, 1, -1, assertionCount);

	    		System.out.println("TEST: (A dropped index down by 1) Success ");

	    		bResponse = serverB.receiveAppendEntry(6, 0, 0, 1, new Entry[] { new Entry(1, 2) }, -1);
	    		cResponse = serverC.receiveAppendEntry(6, 0, 0, 1, new Entry[] { new Entry(1, 2) }, -1);
	    		dResponse = serverD.receiveAppendEntry(6, 0, 0, 1, new Entry[] { new Entry(1, 2) }, -1);
	    		eResponse = serverE.receiveAppendEntry(6, 0, 0, 1, new Entry[] { new Entry(1, 2) }, -1);

	    		// Server B should accept request and append entry.
	    		PragmaProducer.ExpectAppendEntryResponse("Server B accepts A's append entry request", "B", bResponse, 0, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server C rejects A's append entry request", "C", cResponse, 6, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server D rejects A's append entry request", "D", dResponse, 6, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server E rejects A's append entry request", "E", eResponse, 6, assertionCount);

				System.out.println("TEST: (B accepts append entry) Success ");
				System.out.println(serverB.getCurrentLog().getLastEntryIndex());

	        	// All servers respond to A, and their requests get through. B accepts.
            	appendEntryRequesterA.clearResponsesForTerm(6);
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 1, true, 0));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 2, false, 6));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 3, false, 6));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 4, false, 6));

				System.out.println("TEST: (only B success) Success ");

	    		appendEntryRequesterA.clearRequestsForTerm(6);
            	serverA.handleTimeout();

				System.out.println("TEST: (dropped index down by 1) Success ");

				PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to B", "A", appendEntryRequesterA, 6, 1, 1, 2, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to C", "A", appendEntryRequesterA, 6, 2, -1, -1, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to D", "A", appendEntryRequesterA, 6, 3, -1, -1, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to E", "A", appendEntryRequesterA, 6, 4, -1, -1, -1, assertionCount);

				System.out.println("TEST: (received next set of append entries) Success ");

	    		bResponse = serverB.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, -1);
	    		cResponse = serverC.receiveAppendEntry(6, 0, -1, -1, new Entry[] { new Entry(1, 1), new Entry(1, 2) }, -1);
	    		dResponse = serverD.receiveAppendEntry(6, 0, -1, -1, new Entry[] { new Entry(1, 1), new Entry(1, 2) }, -1);
	    		eResponse = serverE.receiveAppendEntry(6, 0, -1, -1, new Entry[] { new Entry(1, 1), new Entry(1, 2) }, -1);

	    		PragmaProducer.ExpectAppendEntryResponse("Server B accepts A's append entry request", "B", bResponse, 0, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server C accepts A's append entry request", "C", cResponse, 0, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server D accepts A's append entry request", "D", dResponse, 0, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server E accepts A's append entry request", "E", eResponse, 0, assertionCount);

				System.out.println("TEST: (all servers accept) Success ");

	    		appendEntryRequesterA.clearResponsesForTerm(6);
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 1, true, -1));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 2, true, -1));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 3, true, -1));
	    		appendEntryRequesterA.addAppendResponseForTerm(6, new AppendResponse(0, 4, true, -1));

				System.out.println("TEST: (all responses sent) Success ");

				appendEntryRequesterA.clearRequestsForTerm(6);
	    		serverA.handleTimeout();

				System.out.println("TEST: (A sees success responses) Success ");

	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to B", "A", appendEntryRequesterA, 6, 1, 1, 2, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to C", "A", appendEntryRequesterA, 6, 2, 1, 2, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to D", "A", appendEntryRequesterA, 6, 3, 1, 2, -1, assertionCount);
	    		PragmaProducer.ExpectAppendEntryRequest("Server A sent append entry requests to E", "A", appendEntryRequesterA, 6, 4, 1, 2, -1, assertionCount);

				System.out.println("TEST: (A sending heartbeat) Success ");
				//	    	    PragmaProducer.ExpectCommitIndex("Server B commit index", "B", serverB.getCommitIndex(), -1, assertionCount);
				//            PragmaProducer.ExpectCommitIndex("Server C commit index", "C", serverC.getCommitIndex(), -1, assertionCount);
				//            PragmaProducer.ExpectCommitIndex("Server D commit index", "D", serverD.getCommitIndex(), -1, assertionCount);
				//            PragmaProducer.ExpectCommitIndex("Server E commit index", "E", serverE.getCommitIndex(), -1, assertionCount);

	    		bResponse = serverB.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, 1);
	    		cResponse = serverC.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, 1);
	    		dResponse = serverD.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, 1);
	    		eResponse = serverE.receiveAppendEntry(6, 0, 1, 2, new Entry[] {}, 1);


	    		PragmaProducer.ExpectAppendEntryResponse("Server B accepts A's append entry request", "B", bResponse, 0, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server C accepts A's append entry request", "C", cResponse, 0, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server D accepts A's append entry request", "D", dResponse, 0, assertionCount);
            	PragmaProducer.ExpectAppendEntryResponse("Server E accepts A's append entry request", "E", eResponse, 0, assertionCount);
				System.out.println("TEST: (heartbeat success) Success ");
	    //            PragmaProducer.ExpectCommitIndex("Server B commit index", "B", serverB.getCommitIndex(), 1, assertionCount);
	    //            PragmaProducer.ExpectCommitIndex("Server C commit index", "C", serverC.getCommitIndex(), 1, assertionCount);
	    //            PragmaProducer.ExpectCommitIndex("Server D commit index", "D", serverD.getCommitIndex(), 1, assertionCount);
	    //            PragmaProducer.ExpectCommitIndex("Server E commit index", "E", serverE.getCommitIndex(), 1, assertionCount);

            System.out.println("@ag350:assertion:start");
            String assertion = String.format("@ag350:assertion:data { \"name\": \"Protocol\", \"status\": \"PASS\", \"data\": { } }");
            System.out.println(assertion);
            System.out.println("@ag350:assertion:end");

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
