/**
 * Copyright 2015 FSB Technology (UK) Ltd ("FSB")
 *
 * The contents of this file are the property of FSB. You may not use the contents of this file without the express
 * permission of FSB.
 *
 */

import java.math.BigDecimal;
import java.util.Date;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

//import com.fsbtech.bet.type.MarketState;

/**
 * @author luqman on 16 Nov 2015
 */
public class MarketVO extends FSBObjectWithId<MarketIdVO>
{

    private static final long serialVersionUID = 1L;

    @QuerySqlField
    private long marketTypeId;

    @QuerySqlField(index = true)
    private long eventId;
    private int version;

    // these fields can be changed and will only have setters available.
//    @QuerySqlField
//    private MarketState marketState;
    private Date stopBettingOn;
    private boolean isActive;
    private boolean inRunning;
    private String name;
    @QuerySqlField
    private long sequence;
    private String remoteReference;
    private String metadata;
    private BigDecimal handicap;
    private Long superSiteId;

    /**
     * Need default constructor for myBatis
     */
    public MarketVO()
    {
        super(new MarketIdVO());
    }

    /**
     * @param eventId
     *            Foreign key
     * @param marketTypeId
     *            Foreign key
//     * @param marketState
//     *            Foreign key
     * @param stopBettingOn
     * @param version
     * @param isActive
     * @param inRunning
     * @param name
     * @param sequence
     * @param handicap
     * @param remoteReference
     *            Could be null if no remote reference is available
     * @param metadata
     *            Could be null in case no metadata info available
     * @param superSiteId
     */
    public MarketVO(long eventId, long marketTypeId, Date stopBettingOn,
        int version, boolean isActive, boolean inRunning, String name, long sequence, BigDecimal handicap,
        String remoteReference, String metadata, Long superSiteId)
    {
        this.eventId = eventId;
        this.marketTypeId = marketTypeId;
        //this.marketState = marketState;
        this.stopBettingOn = stopBettingOn;
        this.version = version;
        this.isActive = isActive;
        this.inRunning = inRunning;
        this.name = name;
        this.sequence = sequence;
        this.handicap = handicap;
        this.remoteReference = remoteReference;
        this.metadata = metadata;
        this.superSiteId = superSiteId;
    }

    /**
     * @return the marketTypeId
     */
    public long getMarketTypeId()
    {
        return marketTypeId;
    }

    /**
     * @return the eventId
     */
    public long getEventId()
    {
        return eventId;
    }

//    /**
//     * @return the marketState
//     */
//    public MarketState getMarketState()
//    {
//        return marketState;
//    }
//
//    /**
//     * @param marketState
//     *            the marketState to set
//     */
//    public void setMarketState(MarketState marketState)
//    {
//        this.marketState = marketState;
//    }

    /**
     * @return the stopBettingOn
     */
    public Date getStopBettingOn()
    {
        return stopBettingOn;
    }

    /**
     * @param stopBettingOn
     *            the stopBettingOn to set
     */
    public void setStopBettingOn(Date stopBettingOn)
    {
        this.stopBettingOn = stopBettingOn;
    }

    /**
     * @return the version
     */
    public int getVersion()
    {
        return version;
    }

    /**
     * @return the isActive
     */
    public boolean isActive()
    {
        return isActive;
    }

    /**
     * @param isActive
     *            the isActive to set
     */
    public void setActive(boolean isActive)
    {
        this.isActive = isActive;
    }

    /**
     * @return the inRunning
     */
    public boolean isInRunning()
    {
        return inRunning;
    }

    /**
     * @param inRunning
     *            the inRunning to set
     */
    public void setInRunning(boolean inRunning)
    {
        this.inRunning = inRunning;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * @return the sequence
     */
    public long getSequence()
    {
        return sequence;
    }

    /**
     * @param sequence
     *            the sequence to set
     */
    public void setSequence(long sequence)
    {
        this.sequence = sequence;
    }

    /**
     * @return the remoteReference
     */
    public String getRemoteReference()
    {
        return remoteReference;
    }

    /**
     * @param remoteReference
     *            the remoteReference to set
     */
    public void setRemoteReference(String remoteReference)
    {
        this.remoteReference = remoteReference;
    }

    /**
     * @return the metadata
     */
    public String getMetadata()
    {
        return metadata;
    }

    /**
     * @param metadata
     *            the metadata to set
     */
    public void setMetadata(String metadata)
    {
        this.metadata = metadata;
    }

    /**
     * @return the handicap
     */
    public BigDecimal getHandicap()
    {
        return handicap;
    }

    /**
     * @param handicap
     *            the handicap to set
     */
    public void setHandicap(BigDecimal handicap)
    {
        this.handicap = handicap;
    }

    /**
     * @return the superSiteId
     */
    public Long getSuperSiteId()
    {
        return superSiteId;
    }

    /**
     * @param superSiteId
     *            the superSiteId to set
     */
    public void setSuperSiteId(Long superSiteId)
    {
        this.superSiteId = superSiteId;
    }

    @Override
    @QuerySqlField(index = true)
    public MarketIdVO getId()
    {
        return super.getId();
    }

    @Override
    public String toString()
    {
        return "MarketVO [" + getId() + ", marketTypeId=" + marketTypeId + ", eventId=" + eventId
            + ", stopBettingOn=" + stopBettingOn + ", version=" + version + ", isActive=" + isActive
                + ", inRunning=" + inRunning + ", name=" + name + ", sequence=" + sequence + ", remoteReference="
                + remoteReference + ", metadata=" + metadata + ", superSiteId=" + superSiteId + "]";
    }

}