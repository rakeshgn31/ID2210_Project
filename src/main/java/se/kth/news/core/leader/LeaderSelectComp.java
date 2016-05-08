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
import se.kth.news.core.news.NewsComp;
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
    private KAddress selfAdr;
    //*******************************INTERNAL_STATE*****************************
    private UUID m_nLETimeoutID;
    private KAddress m_addrLeader;
    private boolean m_bLeader, m_bIsLeaderAlive;
    private int m_nNumOfACKsForLE;
    private TGradientSample<NewsView> tempGradSample;
    private TGradientSample<NewsView> gradSample;
    private Comparator viewComparator;

    public LeaderSelectComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);
        
        m_addrLeader = null;
        gradSample = tempGradSample = null;        
        m_bLeader = m_bIsLeaderAlive = false;
        m_nNumOfACKsForLE = 0;
        viewComparator = init.viewComparator;

        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            
            // Schedule a timeout for the leader election and initial topology stabilization
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(0, 30000);
            LETimeout leTO = new LETimeout(spt);
            spt.setTimeoutEvent(leTO);
            trigger(spt, timerPort);
            m_nLETimeoutID = leTO.getTimeoutId();  
        }                      
    };
    
    @Override
    public void tearDown() {
        trigger(new CancelPeriodicTimeout(m_nLETimeoutID), timerPort);
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

    Handler handleLETimeout = new Handler<LETimeout>() {
        @Override
        public void handle(LETimeout event) {
            if(m_addrLeader == null && !m_bIsLeaderAlive && gradSample != null) {
                // No leader is present, so conduct the leader election
                int nCounter = 0;
                tempGradSample = gradSample;
                m_nNumOfACKsForLE = 0;
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
            if(tempGradSample != null) {
                int nReqAcceptance = ( (int)Math.floor((double)tempGradSample.gradientNeighbours.size()/2) + 1 );
                if(m_nNumOfACKsForLE >= nReqAcceptance) {
                    
                    // Majority is reached and hence the node is the elected leader
                    m_addrLeader = selfAdr;
                    m_bLeader = true;
                    m_bIsLeaderAlive = true;
                    
                    // Trigger the Leader dissemination
                } 
            }
        }            
    };
    
    public static class LETimeout extends Timeout {
        
        public LETimeout(SchedulePeriodicTimeout spt) {
            super(spt);
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
