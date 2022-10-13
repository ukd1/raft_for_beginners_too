# Raft for Beginngers

## Usage
To start a test "cluster" of 3 nodes, run:

```bash
eval "$(ruby run.rb)"
```

You can then send SIGQUIT to kill the leader `<ctrl>+\`, and or SIGINT to kill all `<ctrl>+c`.

## TODO

- [ ] add election timeout min / max range, and randomly wait before calling elections
- [ ] validate timeout cli inputs
- [ ] add term
- [ ] send election packets
- [ ] recv election packets, and decide on new leader


- [ ] Raft uses a heartbeat mechanism to trigger leader election. 
- [ ] When servers start up, they begin as followers. 
- [ ] A server remains in follower state as long as it receives valid RPCs from a leader or candidate.
- [ ] Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries) to all followers in order to maintain their authority. 
- [ ] If a follower receives no communication over a period of time called the election timeout, then it assumes there is no viable leader and begins an election to choose a new leader.
- [ ] To begin an election, 
 - [ ] and transitions to candidate state. 
 - [ ] a follower increments its current term
 - [ ] It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
   - [ ] Note `in parallel to each of the other servers`, we do this in a loop...but meh?
- [ ] A candidate continues in this state until one of three things happens: 
  - [ ] (a) it wins the election, 
  - [ ] (b) another server establishes itself as leader,
  - [ ] or (c) a period of time goes by with no winner.

These outcomes are discussed separately in the paragraphs below.

- [ ] A candidate wins an election if it receives votes from a majority of the servers in the full cluster for the same term. 
- [ ] Each server will vote for at most one candidate in a given term, on a first-come-first-served basis (note: Section 5.4 adds an additional restriction on votes). 

The majority rule ensures that at most one candidate can win the election for a particular term (the Election Safety Property in Figure 3). 
- [ ] Once a candidate wins an election, it becomes leader. 
- [ ] It then sends heartbeat messages to all of the other servers to establish its authority and prevent new elections.
- [ ] While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader. 
- [ ] If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate recognizes the leader as legitimate and returns to follower state. 
- [ ] If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.

- The third possible outcome is that a candidate neither wins nor loses the election: if many followers become candidates at the same time, votes could be split so that no candidate obtains a majority.
- [ ] When this happens, each candidate will time out and start a new election by incrementing its term and initiating another round of Request-Vote RPCs. 
- However, without extra measures split votes could repeat indefinitely.
  - Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly. 
  - [ ] To prevent split votes in the first place, election timeouts are chosen randomly from a fixed interval (e.g., 150–300ms). 
- This spreads out the servers so that in most cases only a single server will time out; it wins the election and sends heartbeats before any other servers time out. 
- The same mechanism is used to handle split votes. 
- Each candidate restarts its randomized election timeout at the start of an election, and it waits for that timeout to elapse before starting the next election; this reduces the likelihood of another split vote in the new election. 
