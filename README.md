-----------------------------------------------------------------------------------------------------------------------------------------
                                    Peer-to-Peer Sampling Service and Data Exchange
-----------------------------------------------------------------------------------------------------------------------------------------
This project is to build a peer-to-peer using the croupier peer sampling service and gradient overlay topology. 
Initial information dissemination is implemented using Flooding until a leader is elected. 
Leader is elected using the Peer ID and the Maximum information received by the Peers with this metadata exchange facilitated by Two-phase commit protocol. 
Leader update happens with Push-Pull mechanisms using neighbor peers and Finger nodes provided by Gradient topology. 
Failure detection is based on timeouts and replies to the queries. 
