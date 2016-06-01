/*
 * 2016 Royal Institute of Technology (KTH)
 *
 * LSelector is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.news.core.leader;

import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.news.LeaderFailureUpdate;
import se.kth.news.core.news.LeaderFailureUpdatePort;
import se.kth.news.core.news.util.NewsView;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.other.Container;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeaderSelectComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSelectComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Negative<LeadeUpdatePort> leaderUpdatePort = provides(LeadeUpdatePort.class);
    Positive<LeaderFailureUpdatePort> leaderFailureUpdatePort = requires(LeaderFailureUpdatePort.class);
    //*******************************EXTERNAL_STATE*****************************
    
    //*******************************INTERNAL_STATE*****************************
    private KAddress selfAdr;
    
    // Task 2 related variables
    private KAddress m_addrLeader, m_addrProbableLeader;    
    private int m_nNumOfNomineeResp, m_nNumOfCommitResp; 
    private TGradientSample<NewsView> gradSample;
    private TGradientSample<NewsView> tempGradSample;
    private Comparator viewComparator;
    private int nReqNomineeResponses, nReqCommitResponses;
    
    // Task 3 related variables
    private UUID m_nLUTimeoutID;
    private boolean m_bLeader, m_bLeaderDisseminated, m_bIsLeaderAlive;

    public LeaderSelectComp(Init init) {
        
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);
        
        gradSample = null;
        viewComparator = init.viewComparator;
        m_addrLeader = m_addrProbableLeader = null;
        m_bLeader = m_bLeaderDisseminated = m_bIsLeaderAlive = false;   
        nReqNomineeResponses = nReqCommitResponses = 0;
        
        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
        
        // Task - 2 related events
        subscribe(handleGradStabTimeout, timerPort);
        subscribe(handleLENomineeReq, networkPort);
        subscribe(handleLENominationResp, networkPort);
        subscribe(handleLeaderCommitReq, networkPort);
        subscribe(handleLeaderCommitResp, networkPort);
        
        // Task - 3.1 related events
        subscribe(handleLUTimeout, timerPort);
        subscribe(handleGetLeaderReq, networkPort);
        subscribe(handleGetLeaderResp, networkPort);
        
        // Task - 4.1
        subscribe(handleLeaderFailureUpdate, leaderFailureUpdatePort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            
            // Schedule a timeout so that the topology shall stabilize by then
            ScheduleTimeout spt = new ScheduleTimeout(210000);
            GradientStabilizationTimeout gradStabTO = new GradientStabilizationTimeout(spt);
            spt.setTimeoutEvent(gradStabTO);
            trigger(spt, timerPort);
        }                      
    };
    
    @Override
    public void tearDown() {
        if(m_nLUTimeoutID != null) {
            
            trigger(new CancelPeriodicTimeout(m_nLUTimeoutID), timerPort);
        }
    }
    
    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {            
            gradSample = sample;
        }
    };

    Handler handleGradStabTimeout = new Handler<GradientStabilizationTimeout>() {
        @Override
        public void handle(GradientStabilizationTimeout e) {
            
            // Schedule a periodic timeout for either leader election or to poll for leader update
            SchedulePeriodicTimeout spt1 = new SchedulePeriodicTimeout(2000, 30000);
            UpdateLeaderTimeout luTO = new UpdateLeaderTimeout(spt1);
            spt1.setTimeoutEvent(luTO);
            trigger(spt1, timerPort);
            m_nLUTimeoutID = luTO.getTimeoutId();
        }        
    };
    
    // *************************************************************************
    //                      Leader Update Code
    // *************************************************************************
    Handler handleLUTimeout = new Handler<UpdateLeaderTimeout>() {
        @Override
        public void handle(UpdateLeaderTimeout e) {
            
            if(m_addrLeader == null && m_bIsLeaderAlive == false) {
                
                boolean bCan_I_BeLeader = checkIfICanBeLeader();
                if(bCan_I_BeLeader == true) {

                    tempGradSample = gradSample;
                    m_bLeaderDisseminated = false;
                    m_nNumOfNomineeResp = m_nNumOfCommitResp = 0;
                    nReqNomineeResponses = tempGradSample.gradientNeighbours.size();
                    Iterator<Container<KAddress, NewsView>> iter2 = tempGradSample.gradientNeighbours.iterator();
                    while(iter2.hasNext()) {
                        KAddress neighbor = iter2.next().getSource();
                        KHeader header = new BasicHeader(selfAdr, neighbor, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, new LeaderNomineeRequest(tempGradSample.selfView));
                        trigger(msg, networkPort);
                    } 
                } else {

                    // Get the leader from the highest finger node because the 
                    // highest finger will be closer to the center of the network
                    Container<KAddress, NewsView> highestFingerNode = getNodeWithHighestNewsview(false);
                    if(highestFingerNode != null) {
                        KHeader header = new BasicHeader(selfAdr, highestFingerNode.getSource(), Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, new GetLeaderRequest());
                        trigger(msg, networkPort);
                    }
                }
            }
        }                
    };

    // News component triggered leader failure event is handled here
    Handler handleLeaderFailureUpdate = new Handler<LeaderFailureUpdate>() {
        @Override
        public void handle(LeaderFailureUpdate e) {
            
            m_addrLeader = null;
            m_bLeader = m_bIsLeaderAlive = false;
        }        
    };
    
    private void updateLeader(KAddress addrLeader) {
        
        m_addrLeader = addrLeader;
        m_bIsLeaderAlive = true;
        LOG.info(selfAdr.getIp().toString() + " setting " + m_addrLeader.getIp().toString() + " as leader.");

        // Update the news component of the leader (first time) or the new leader
        trigger(new LeaderUpdate(m_addrLeader), leaderUpdatePort);
    }

    private boolean checkIfICanBeLeader() {

        boolean bReturnValue = false;

        // Compare the highest valued neighbor with node's own view 
        if(gradSample != null) {
            if(viewComparator.compare(gradSample.selfView, getNodeWithHighestNewsview(true).getContent()) > 0) {                    
               bReturnValue = true;
            }
        }
        
        return bReturnValue;
    }
    
    private Container<KAddress, NewsView> getNodeWithHighestNewsview(boolean bgetHighestNeighbor) {
     
        TGradientSample<NewsView> tempGradSample = gradSample;
        Container<KAddress, NewsView> node = null;
        
        if(tempGradSample != null) {
            if(bgetHighestNeighbor) {
                // Return the neighbor with the highest news view
                if(tempGradSample.getGradientNeighbours() != null) {
                    if(tempGradSample.gradientNeighbours.size() > 0) {
                        node = tempGradSample.getGradientNeighbours().get(0);
                        for(int nIndex = 1; nIndex < tempGradSample.getGradientNeighbours().size(); nIndex++) {
                            if(viewComparator.compare(node.getContent(), tempGradSample.getGradientNeighbours().get(nIndex).getContent()) < 0) {
                                node = tempGradSample.getGradientNeighbours().get(nIndex);
                            }            
                        }
                    }
                }
            } else {
                // Get highest Finger
                if(tempGradSample.getGradientFingers() != null) {
                    if(tempGradSample.getGradientFingers().size() > 0) {
                        node = tempGradSample.getGradientFingers().get(0);
                        for(int nIndex = 1; nIndex < tempGradSample.getGradientFingers().size(); nIndex++) {
                            if(viewComparator.compare(node.getContent(), tempGradSample.getGradientFingers().get(nIndex).getContent()) < 0) {
                                node = tempGradSample.getGradientFingers().get(nIndex);
                            }            
                        }
                    }
                }
            }
        }
        
        return node;
    }
    
    ClassMatchedHandler handleLENomineeReq = new 
        ClassMatchedHandler<LeaderNomineeRequest, KContentMsg<?, ?, LeaderNomineeRequest>>() {
        @Override
        public void handle(LeaderNomineeRequest req, KContentMsg<?, ?, LeaderNomineeRequest> container) {
            if(gradSample != null) {
                
                boolean bSendACK = false;
                if( viewComparator.compare(gradSample.selfView, req.m_nomineeNewsView) < 0) {
                    
                    Container<KAddress, NewsView> highestNeighbor = getNodeWithHighestNewsview(true);
                    if( viewComparator.compare(highestNeighbor.getContent(), req.m_nomineeNewsView) < 0) {
                        bSendACK = true;
                    }
                }
              
                // Finally send the acknowledgement
                if(bSendACK == true) {
                    
                    m_addrProbableLeader = container.getHeader().getSource();
                }
                KHeader header = new BasicHeader(selfAdr, container.getHeader().getSource(), Transport.UDP);
                KContentMsg msg = new BasicContentMsg(header, new LeaderNominationResponse(bSendACK));
                trigger(msg, networkPort);
            }
        }            
    };
      
    ClassMatchedHandler handleLENominationResp = new 
        ClassMatchedHandler<LeaderNominationResponse, KContentMsg<?, ?, LeaderNominationResponse>>() {
        @Override
        public void handle(LeaderNominationResponse resp, KContentMsg<?, ?, LeaderNominationResponse> container) {
            
            if(resp.bNodeAgreement) {
                m_nNumOfNomineeResp++;
            }

            // Temp Gradient Sample is already assigned during LE nominee 
            // request trigger. Checking it to be double safe
            // if(tempGradSample != null && m_bLeaderDisseminated == false) {
            if(m_nNumOfNomineeResp == nReqNomineeResponses) {
                sendCommitRequestsToNeighbors();                                        
            } 
        }            
    };        
    
    private void sendCommitRequestsToNeighbors() {
        
        LOG.info(selfAdr.getIp().toString() + " sending commit requests to my neighbors to be elected as leader.");
        
        // Update the leader to immediate neighbours
        if(tempGradSample.gradientNeighbours.size() > 0) {
            nReqCommitResponses = tempGradSample.gradientNeighbours.size();
            Iterator<Container<KAddress, NewsView>> iter = tempGradSample.gradientNeighbours.iterator();
            while(iter.hasNext()) {

                KAddress neighborAddr = iter.next().getSource();
                KHeader header = new BasicHeader(selfAdr, neighborAddr, Transport.UDP);
                KContentMsg msg = new BasicContentMsg(header, new LeaderCommitRequest());
                LOG.info(selfAdr.getIp().toString() + " sending commit req to " + neighborAddr.getIp().toString());
                trigger(msg, networkPort);
            }
        }
    }
    
    ClassMatchedHandler handleLeaderCommitReq = new 
            ClassMatchedHandler<LeaderCommitRequest, KContentMsg<?, ?, LeaderCommitRequest>>() {
        @Override
        public void handle(LeaderCommitRequest req, KContentMsg<?, ?, LeaderCommitRequest> msg) {
            
            // Check with the previous nominee request - probable leader
            KHeader header = new BasicHeader(selfAdr, msg.getHeader().getSource(), Transport.UDP);
            KContentMsg respMsg;
            if(m_addrProbableLeader.getId().equals(msg.getHeader().getSource().getId())) {
                
                LOG.info(selfAdr.getIp().toString() + " resp to commit Req with ACK");
                respMsg = new BasicContentMsg(header, new LeaderCommitResponse(true));

                // As it matches, the requested node can be accepted as the leader
                updateLeader(msg.getHeader().getSource());
            } else {
                
                respMsg = new BasicContentMsg(header, new LeaderCommitResponse(false));
            }
            
            // Trigger the response
            trigger(respMsg, networkPort);
        }          
    };
    
    ClassMatchedHandler handleLeaderCommitResp = new 
            ClassMatchedHandler<LeaderCommitResponse, KContentMsg<?, ?, LeaderCommitResponse>>() {
        @Override
        public void handle(LeaderCommitResponse resp, KContentMsg<?, ?, LeaderCommitResponse> msg) {
            
            if(resp.bNodeAgreement) {
                m_nNumOfCommitResp++;
            }
            
            if(m_nNumOfCommitResp == nReqCommitResponses) {

                // All nodes have agreed and hence I am the leader now
                // I shall inform my news view component of the update
                m_addrLeader = selfAdr;
                m_bLeader = m_bIsLeaderAlive = true;
                m_bLeaderDisseminated = true;
                
                // createLeaderNodeIDFile();
                LOG.info(selfAdr.getIp().toString() + " is the LEADER now..!!!");
                
                // Set the leader
                updateLeader(selfAdr);
            }
        }          
    };
    
    // For leader kill simulation
    private void createLeaderNodeIDFile() {
        
        try {
                String leaderNodeId = selfAdr.getIp().toString().split("\\.")[3];

                // if file exists, first delete it and then create it
                File file = new File("E:\\leader.txt");
                if (file.exists()) {
                    file.delete();
                }                
                file.createNewFile();

                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(leaderNodeId);
                bw.close();
                LOG.info("Created file with leader node ID for KILL simulation");
        } catch (IOException e) {}
    }
    
    ClassMatchedHandler handleGetLeaderReq = new 
        ClassMatchedHandler<GetLeaderRequest, KContentMsg<?, ?, GetLeaderRequest>>() {
            @Override
            public void handle(GetLeaderRequest req, KContentMsg<?, ?, GetLeaderRequest> content) {
                if(m_addrLeader != null) {
                    
                    LOG.info(selfAdr.getIp().toString() + " got a request for leader from " 
                                            + content.getHeader().getSource().getIp().toString());
                    KHeader header = new BasicHeader(selfAdr, content.getHeader().getSource(), Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, new GetLeaderResponse(m_addrLeader));
                    trigger(msg, networkPort);
                }
            }
        };

    ClassMatchedHandler handleGetLeaderResp = new 
                    ClassMatchedHandler<GetLeaderResponse, KContentMsg<?, ?, GetLeaderResponse>>() {
        @Override
        public void handle(GetLeaderResponse resp, KContentMsg<?, ?, GetLeaderResponse> content) {

            // Compare to check if the leader has not changed
            if(resp.m_addrLeader != null) {
                
                // Because it got the leader response from others
                m_bLeader = false;
                updateLeader(resp.m_addrLeader);
            }
        }
    };
        
    // *************************************************************************
    //                     Timers and Initialization classes
    // *************************************************************************
    public static class GradientStabilizationTimeout extends Timeout {
        
        public GradientStabilizationTimeout(ScheduleTimeout spt) {
            super(spt);
        }
    }
    
    public static class UpdateLeaderTimeout extends Timeout {
        
        public UpdateLeaderTimeout(SchedulePeriodicTimeout spt1) {
            super(spt1);
        }
    }
    
    public static class Init extends se.sics.kompics.Init<LeaderSelectComp> {

        public final KAddress selfAdr;
        public final Comparator viewComparator;

        public Init(KAddress selfAdr, Comparator viewComparator) {
            this.selfAdr = selfAdr;
            this.viewComparator = viewComparator;
        }
    }
}
