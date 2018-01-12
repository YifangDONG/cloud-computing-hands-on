# Majority Voting Algorithm (1, N)

- Assum majority of correct processes
	- Register values have a sequence number (seq#) : (value, seq)
	- asynchronous
```
to write(v)
	seq# ++
	Broadcast v and seq# to all
		if newer seq#: // means Receiver receive (v,seq#) and seq# is bigger than it's local seq#'
			Receiver update to (v, seq#)
			Receiver sends ACK
	Wait for ACK from majority of nodes
	Return

to read
	Broadcast read request to all
		Receiver respond with local value and seq#
	Wait and save values from majority of nodes
	return value with highest seq#
```
- probleme with majority voting regular but not atomic

# Read-impose write majority (1, N)
- Read-impose
	- when reading, also make a write before responding

```
to read
	Broadcast read request to all
		Receiver respond with local value and seq#
	Wait and save values from majority of nodes
	Write value with highest seq#	
	return value written	
```

