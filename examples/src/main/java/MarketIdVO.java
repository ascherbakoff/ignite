/**
 * Copyright 2015 FSB Technology (UK) Ltd ("FSB") The contents of this file are the property of FSB. You may not use the
 * contents of this file without the express permission of FSB.
 */

//import com.fsbtech.vo.common.FSBIdVO;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * @author luqman on 16 Nov 2015
 */
public class MarketIdVO
{
    private static final long serialVersionUID = 1L;

    @QuerySqlField(index = true)
    private long marketId;

    /**
     * Need default constructor for myBatis
     */
    public MarketIdVO()
    {
        super();
    }

    /**
     * @param marketId
     */
    public MarketIdVO(long marketId)
    {
        this.marketId = marketId;
    }

    /**
     * @return the marketId
     */
    public long getMarketId()
    {
        return marketId;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (marketId ^ (marketId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        MarketIdVO other = (MarketIdVO) obj;
        if (marketId != other.marketId)
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "marketId=" + marketId;
    }

}
