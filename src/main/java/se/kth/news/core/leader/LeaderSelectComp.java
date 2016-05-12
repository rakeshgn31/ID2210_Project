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
    Negative<LeaderSelectPort> leaderUpdate = provides(LeaderSelectPort.class);
    //*******************************EXTERNAL_STATE*****************************
    
    //*******************************INTERNAL_STATE*****************************
    private KAddress selfAdr;
    
    // Task 2 related variables
    private UUID m_nLETimeoutID;
    private KAddress m_addrLeader;    
    private int m_nNumOfACKsForLE;
    private TGradientSample<NewsView> tempGradSample;
    private TGradientSample<NewsView> gradSample;
    private Comparator viewComparator;
    private boolean m_bLeader, m_bLeaderDisseminated, m_bIsLeaderAlive;
    
    // Task 3 related variables
    private UUID m_nLUTimeoutID;
    private int m_nMinResponses;
    private int m_nCountFingerResponses;
    private KAddress m_addrNewLeader;

    public LeaderSelectComp(Init init) {
        
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);
        
        gradSample = tempGradSample = null;
        m_addrLeader = m_addrNewLeader = null;
        viewComparator = init.viewComparator;
        m_bLeader = m_bLeaderDisseminated = m_bIsLeaderAlive = false;
        m_nNumOfACKsForLE = m_nCountFingerResponses = m_nMinResponses = 0;        

        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
        
        subscribe(handleLETimeout, timerPort);
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
            
            // Schedule a timeout for the leader election and initial topology stabilization
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(0, 60000);
            LETimeout leTO = new LETimeout(spt);
            spt.setTimeoutEvent(leTO);
            trigger(spt, timerPort);
            m_nLETimeoutID = leTO.getTimeoutId();  
            
            // Schedule a periodic timeout for getting to learn the leader
            SchedulePeriodicTimeout spt1 = new SchedulePeriodicTimeout(0, 20000);
            LUpdateTimeout luTO = new LUpdateTimeout(spt1);
            spt1.setTimeoutEvent(luTO);
            trigger(spt1, timerPort);
            m_nLUTimeoutID = luTO.getTimeoutId();
        }                      
    };
    
    @Override
    public void tearDown() {
        trigger(new CancelPeriodicTimeout(m_nLETimeoutID), timerPort);
        trigger(new CancelPeriodicTimeout(m_nLUTimeoutID), timerPort);
    }
    
    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
            LOG.debug("{}neighbours:{}", logPrefix, sample.gradientNeighbours);
            LOG.debug("{}fingers:{}", logPrefix, sample.gradientFingers);
            LOG.debug("{}local view:{}", logPrefix, sample.selfView);
            
            gradSample = sample;
        }
    };

    // *************************************************************************
    //                      Leader Election Code
    // *************************************************************************
    Handler handleLETimeout = new Handler<LETimeout>() {
        @Override
        public void handle(LETimeout event) {
            if(m_addrLeader == null && !m_bIsLeaderAlive && gradSample != null) {
                LOG.info(selfAdr.getIp().toString() + ": leader is empty so gonna conduct election");
                // No leader is present, so conduct the leader election
                int nCounter = 0;
                tempGradSample = gradSample;
                m_nNumOfACKsForLE = 0;
                m_bLeaderDisseminated = false;
                if(tempGradSample.gradientNeighbours.size() > 0) {
                    Iterator<Container<KAddress, NewsView>> iter1 = 
                                tempGradSample.gradientNeighbours.iterator();
                    while(iter1.hasNext()) {
                        if(viewComparator.compare(tempGradSample.selfView, iter1.next().getContent()) >= 0) {
                           nCounter++;
                        } 
                    }
                    
                    // Utility value of the node is greater than its neighbor.
                    // Hence, it triggers a Leader Nominee request to all its neighbors.
                    if(nCounter == tempGradSample.gradientNeighbours.size()) {
                        LOG.info(selfAdr.getIp().toString() + ": have more utility value - gonna ask my neighbors");
                        Iterator<Container<KAddress, NewsView>> iter2 = tempGradSample.gradientNeighbours.iterator();
                        while(iter2.hasNext()) {
                            KAddress neighbor = iter2.next().getSource();
                            KHeader header = new BasicHeader(selfAdr, neighbor, Transport.UDP);
                            KContentMsg msg = new BasicContentMsg(header, new LeaderNomineeRequest(tempGradSample.selfView));
                            trigger(msg, networkPort);
                        } 
                    }
                }
            }
        }        
    };

        ClassMatchedHandler handleLENomineeReq = new 
        ClassMatchedHandler<LeaderNomineeRequest, KContentMsg<?, ?, LeaderNomineeRequest>>() {
        @Override
        public void handle(LeaderNomineeRequest req, KContentMsg<?, ?, LeaderNomineeRequest> container) {
            if(gradSample != null) {
                tempGradSample = gradSample;
                int nSuccessfulComparisons = 0;
                boolean bSendACK = false;
                // First compare to itself and proceed with comaparing to neighbors
                if( viewComparator.compare(tempGradSample.selfView, req.m_nomineeNewsView) <= 0) {
                    
                    nSuccessfulComparisons++;   // For the successful comparison with received node's view
                    Iterator<Container<KAddress, NewsView>> iter = tempGradSample.gradientNeighbours.iterator();
                    while(iter.hasNext()) {
                        if(viewComparator.compare(iter.next().getContent(), req.m_nomineeNewsView) <= 0) {
                           nSuccessfulComparisons++;
                        }
                    }
                    
                    if(nSuccessfulComparisons == (tempGradSample.gradientNeighbours.size() + 1) ) {
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
                    
                    LOG.info(selfAdr.getIp().toString() + " got majority of acceptance. I am the leader now");
                    // Majority is reached and hence the node is the elected leader
                    m_addrLeader = selfAdr;
                    m_bLeader = m_bLeaderDisseminated = m_bIsLeaderAlive = true;
                    
                    // Trigger the Leader dissemination to fingers
                    // Other nodes shall get the leader from the finger nodes
                    if(tempGradSample.gradientFingers.size() > 0) {
                        Iterator<Container<KAddress, NewsView>> iter = tempGradSample.gradientFingers.iterator();
                        while(iter.hasNext()) {
                            KHeader header = new BasicHeader(selfAdr, iter.next().getSource(), Transport.UDP);
                            KContentMsg msg = new BasicContentMsg(header, new LeaderUpdate(selfAdr));
                            trigger(msg, networkPort);
                        }
                    }
                } 
            }
        }            
    };
        
    // *************************************************************************
    //                      Leader Update Code
    // *************************************************************************
    Handler handleLUTimeout = new Handler<LUpdateTimeout>() {
        @Override
        public void handle(LUpdateTimeout e) {
            
            if( !m_bLeader ) {
                TGradientSample<NewsView> tmpView = gradSample;
                if(tmpView != null) {
                    if(tmpView.gradientFingers.size() > 0) {
                        m_addrNewLeader = null;
                        m_nCountFingerResponses = 0;
                        m_nMinResponses = (int)Math.floor( (double)tmpView.gradientFingers.size()/2.0 ) + 1;

                        Iterator<Container<KAddress, NewsView>> iter = tmpView.gradientFingers.iterator();
                        while(iter.hasNext()) {
                            
                            // Request for the leader to the fingers
                            KHeader header = new BasicHeader(selfAdr, iter.next().getSource(), Transport.UDP);
                            KContentMsg msg = new BasicContentMsg(header, new GetLeaderRequest());
                            trigger(msg, networkPort);
                        }
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
            if(m_addrNewLeader == null) {
                m_addrNewLeader = resp.m_addrLeader;
            } else {
                if( resp.m_addrLeader.getId().equals(m_addrNewLeader.getId()) ) {
                    m_nCountFingerResponses++;
                }
            }
                        
            // Once, you get the same leader from majority of the fingers 
            // then update the leader address locally
            if(m_nCountFingerResponses >= m_nMinResponses) {
                m_addrLeader = m_addrNewLeader;
            }
        }
    };
        
    // *************************************************************************
    //                     Timers and Initialization classes
    // *************************************************************************
    public static class LETimeout extends Timeout {
        
        public LETimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
    
    public static class LUpdateTimeout extends Timeout {
        
        public LUpdateTimeout(SchedulePeriodicTimeout spt1) {
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
