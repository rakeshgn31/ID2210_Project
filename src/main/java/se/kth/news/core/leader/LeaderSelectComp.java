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
    private KAddress m_addrLeader;    
    private int m_nNumOfACKsForLE;
    private TGradientSample<NewsView> tempGradSample;
    private TGradientSample<NewsView> gradSample;
    private Comparator viewComparator;    
    
    // Task 3 related variables
    private UUID m_nLUTimeoutID;
    private boolean m_bLeader, m_bLeaderDisseminated, m_bIsLeaderAlive;

    public LeaderSelectComp(Init init) {
        
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);
        
        gradSample = tempGradSample = null;
        viewComparator = init.viewComparator;
        m_bLeader = m_bLeaderDisseminated = m_bIsLeaderAlive = false;   

        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
        
        subscribe(handleLENomineeReq, networkPort);
        subscribe(handleLENominationResp, networkPort);
        
        subscribe(handleLUTimeout, timerPort);
        subscribe(handleGetLeaderReq, networkPort);
        subscribe(handleGetLeaderResp, networkPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            
            // Schedule a timeout so that the topology shall stabilize by then
            ScheduleTimeout spt = new ScheduleTimeout(70000);
            stabilizationTimeout leTO = new stabilizationTimeout(spt);
            spt.setTimeoutEvent(leTO);
            trigger(spt, timerPort); 
            
            // Schedule a periodic timeout for either leader election or to poll for leader update
            SchedulePeriodicTimeout spt1 = new SchedulePeriodicTimeout(30000, 2000);
            LeaderUpdateTimeout luTO = new LeaderUpdateTimeout(spt1);
            spt1.setTimeoutEvent(luTO);
            trigger(spt1, timerPort);
            m_nLUTimeoutID = luTO.getTimeoutId();
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

    // *************************************************************************
    //                      Leader Update Code
    // *************************************************************************
    Handler handleLUTimeout = new Handler<LeaderUpdateTimeout>() {
        @Override
        public void handle(LeaderUpdateTimeout e) {
            
            if(m_addrLeader == null && m_bIsLeaderAlive == false) {
                
                boolean bCan_I_BeLeader = checkIfICanBeLeader();
                if(bCan_I_BeLeader == true) {

                    LOG.info(selfAdr.getIp().toString() + ": have more utility value - gonna ask my neighbors");
                    tempGradSample = gradSample;
                    m_bLeaderDisseminated = false;
                    m_nNumOfACKsForLE = 0;
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
                    KHeader header = new BasicHeader(selfAdr, highestFingerNode.getSource(), Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, new GetLeaderRequest());
                    trigger(msg, networkPort);
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
        // Update the news component of the leader (first time) or the new leader
        trigger(new LeaderUpdate(m_addrLeader), leaderUpdatePort);
    }

    private boolean checkIfICanBeLeader() {

        boolean bReturnValue = false;

        // Compare the highest valued neighbor with node's own view 
        if(viewComparator.compare(gradSample.selfView, getNodeWithHighestNewsview(true)) > 0) {                    
           bReturnValue = true;
        }             
        
        return bReturnValue;
    }
    
    private Container<KAddress, NewsView> getNodeWithHighestNewsview(boolean bgetHighestNeighbor) {
     
        tempGradSample = gradSample;
        Container<KAddress, NewsView> node = null;
        
        if(bgetHighestNeighbor) {
            // Return the neighbor with the highest news view
            if(tempGradSample.gradientNeighbours.size() > 0) {
                node = tempGradSample.getGradientNeighbours().get(0);
                for(int nIndex = 1; nIndex < tempGradSample.getGradientNeighbours().size(); nIndex++) {
                    if(viewComparator.compare(node.getContent(), tempGradSample.getGradientNeighbours().get(nIndex).getContent()) < 0) {
                        node = tempGradSample.getGradientNeighbours().get(nIndex);
                    }            
                }
            }
        } else {
            // Get highest Finger
            if(tempGradSample.getGradientFingers().size() > 0) {
                node = tempGradSample.getGradientFingers().get(0);
                for(int nIndex = 1; nIndex < tempGradSample.getGradientFingers().size(); nIndex++) {
                    if(viewComparator.compare(node.getContent(), tempGradSample.getGradientFingers().get(nIndex).getContent()) < 0) {
                        node = tempGradSample.getGradientNeighbours().get(nIndex);
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
                LOG.info(selfAdr.getIp().toString() + " sending ACK/NACK to " + container.getHeader().getSource().getIp().toString());
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
                m_nNumOfACKsForLE++;
            }
            
            // Temp Gradient Sample is already assigned during LE nominee 
            // request trigger. Checking it to be double safe
            if(tempGradSample != null && m_bLeaderDisseminated == false) {
                int nReqAcceptance = ( (int)Math.floor((double)tempGradSample.gradientNeighbours.size()/2) + 1 );
                if(m_nNumOfACKsForLE >= nReqAcceptance) {
                    
                    LOG.info(selfAdr.getIp().toString() + " got majority of acceptance. I can be the leader now");
                    // Majority is reached and hence the node is the elected leader
                    m_addrLeader = selfAdr;
                    m_bLeader = m_bIsLeaderAlive = true;
                    
                    // Set the leader
                    updateLeader(selfAdr);
                    
                    // Update the immediate neighbours
                    if(tempGradSample.gradientNeighbours.size() > 0) {
                        Iterator<Container<KAddress, NewsView>> iter = tempGradSample.gradientNeighbours.iterator();
                        while(iter.hasNext()) {
                            
                            KHeader header = new BasicHeader(selfAdr, iter.next().getSource(), Transport.UDP);
                            KContentMsg msg = new BasicContentMsg(header, new LeaderUpdate(selfAdr));
                            trigger(msg, networkPort);
                        }
                        
                        m_bLeaderDisseminated = true;
                    }
                } 
            }
        }            
    };        
    
    ClassMatchedHandler handleGetLeaderReq = new 
        ClassMatchedHandler<GetLeaderRequest, KContentMsg<?, ?, GetLeaderRequest>>() {
            @Override
            public void handle(GetLeaderRequest req, KContentMsg<?, ?, GetLeaderRequest> content) {
                if(m_addrLeader != null) {
                    
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
    public static class stabilizationTimeout extends Timeout {
        
        public stabilizationTimeout(ScheduleTimeout spt) {
            super(spt);
        }
    }
    
    public static class LeaderUpdateTimeout extends Timeout {
        
        public LeaderUpdateTimeout(SchedulePeriodicTimeout spt1) {
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
