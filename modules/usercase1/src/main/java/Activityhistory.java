/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Activityhistory definition.
 *
 * Code generated by Apache Ignite Schema Import utility: 04/29/2016.
 */
public class Activityhistory implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for activityhistoryId. */
    @QuerySqlField(orderedGroups={@QuerySqlField.Group(name = "Activityhistory_idx", order = 1)})
    private long activityhistoryId;

    /** Value for timestamp. */
    private String timestamp;

    /** Value for activityId. */
    private long activityId;

    /** Value for useraccountId. */
    private Long useraccountId;

    /** Value for activitystateEnumid. */
    @QuerySqlField(orderedGroups={@QuerySqlField.Group(name = "Activityhistory_idx", order = 2)})
    private long activitystateEnumid;

    /** Value for remainingdurationunitEnumid. */
    private Long remainingdurationunitEnumid;

    /** Value for usercomment. */
    private String usercomment;

    /** Value for hoursspent. */
    private Short hoursspent;

    /** Value for remainingduration. */
    private Double remainingduration;

    /** Value for sequencenr. */
    private short sequencenr;

    /** Value for logdate. */
    private java.sql.Date logdate;

    /** Value for sessionId. */
    private Long sessionId;

    /**
     * Gets activityhistoryId.
     *
     * @return Value for activityhistoryId.
     */
    public long getActivityhistoryId() {
        return activityhistoryId;
    }

    /**
     * Sets activityhistoryId.
     *
     * @param activityhistoryId New value for activityhistoryId.
     */
    public void setActivityhistoryId(long activityhistoryId) {
        this.activityhistoryId = activityhistoryId;
    }

    /**
     * Gets timestamp.
     *
     * @return Value for timestamp.
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * Sets timestamp.
     *
     * @param timestamp New value for timestamp.
     */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets activityId.
     *
     * @return Value for activityId.
     */
    public long getActivityId() {
        return activityId;
    }

    /**
     * Sets activityId.
     *
     * @param activityId New value for activityId.
     */
    public void setActivityId(long activityId) {
        this.activityId = activityId;
    }

    /**
     * Gets useraccountId.
     *
     * @return Value for useraccountId.
     */
    public Long getUseraccountId() {
        return useraccountId;
    }

    /**
     * Sets useraccountId.
     *
     * @param useraccountId New value for useraccountId.
     */
    public void setUseraccountId(Long useraccountId) {
        this.useraccountId = useraccountId;
    }

    /**
     * Gets activitystateEnumid.
     *
     * @return Value for activitystateEnumid.
     */
    public long getActivitystateEnumid() {
        return activitystateEnumid;
    }

    /**
     * Sets activitystateEnumid.
     *
     * @param activitystateEnumid New value for activitystateEnumid.
     */
    public void setActivitystateEnumid(long activitystateEnumid) {
        this.activitystateEnumid = activitystateEnumid;
    }

    /**
     * Gets remainingdurationunitEnumid.
     *
     * @return Value for remainingdurationunitEnumid.
     */
    public Long getRemainingdurationunitEnumid() {
        return remainingdurationunitEnumid;
    }

    /**
     * Sets remainingdurationunitEnumid.
     *
     * @param remainingdurationunitEnumid New value for remainingdurationunitEnumid.
     */
    public void setRemainingdurationunitEnumid(Long remainingdurationunitEnumid) {
        this.remainingdurationunitEnumid = remainingdurationunitEnumid;
    }

    /**
     * Gets usercomment.
     *
     * @return Value for usercomment.
     */
    public String getUsercomment() {
        return usercomment;
    }

    /**
     * Sets usercomment.
     *
     * @param usercomment New value for usercomment.
     */
    public void setUsercomment(String usercomment) {
        this.usercomment = usercomment;
    }

    /**
     * Gets hoursspent.
     *
     * @return Value for hoursspent.
     */
    public Short getHoursspent() {
        return hoursspent;
    }

    /**
     * Sets hoursspent.
     *
     * @param hoursspent New value for hoursspent.
     */
    public void setHoursspent(Short hoursspent) {
        this.hoursspent = hoursspent;
    }

    /**
     * Gets remainingduration.
     *
     * @return Value for remainingduration.
     */
    public Double getRemainingduration() {
        return remainingduration;
    }

    /**
     * Sets remainingduration.
     *
     * @param remainingduration New value for remainingduration.
     */
    public void setRemainingduration(Double remainingduration) {
        this.remainingduration = remainingduration;
    }

    /**
     * Gets sequencenr.
     *
     * @return Value for sequencenr.
     */
    public short getSequencenr() {
        return sequencenr;
    }

    /**
     * Sets sequencenr.
     *
     * @param sequencenr New value for sequencenr.
     */
    public void setSequencenr(short sequencenr) {
        this.sequencenr = sequencenr;
    }

    /**
     * Gets logdate.
     *
     * @return Value for logdate.
     */
    public java.sql.Date getLogdate() {
        return logdate;
    }

    /**
     * Sets logdate.
     *
     * @param logdate New value for logdate.
     */
    public void setLogdate(java.sql.Date logdate) {
        this.logdate = logdate;
    }

    /**
     * Gets sessionId.
     *
     * @return Value for sessionId.
     */
    public Long getSessionId() {
        return sessionId;
    }

    /**
     * Sets sessionId.
     *
     * @param sessionId New value for sessionId.
     */
    public void setSessionId(Long sessionId) {
        this.sessionId = sessionId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof Activityhistory))
            return false;

        Activityhistory that = (Activityhistory)o;

        if (activityhistoryId != that.activityhistoryId)
            return false;

        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
            return false;

        if (activityId != that.activityId)
            return false;

        if (useraccountId != null ? !useraccountId.equals(that.useraccountId) : that.useraccountId != null)
            return false;

        if (activitystateEnumid != that.activitystateEnumid)
            return false;

        if (remainingdurationunitEnumid != null ? !remainingdurationunitEnumid.equals(that.remainingdurationunitEnumid) : that.remainingdurationunitEnumid != null)
            return false;

        if (usercomment != null ? !usercomment.equals(that.usercomment) : that.usercomment != null)
            return false;

        if (hoursspent != null ? !hoursspent.equals(that.hoursspent) : that.hoursspent != null)
            return false;

        if (remainingduration != null ? !remainingduration.equals(that.remainingduration) : that.remainingduration != null)
            return false;

        if (sequencenr != that.sequencenr)
            return false;

        if (logdate != null ? !logdate.equals(that.logdate) : that.logdate != null)
            return false;

        if (sessionId != null ? !sessionId.equals(that.sessionId) : that.sessionId != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (int)(activityhistoryId ^ (activityhistoryId >>> 32));

        res = 31 * res + (timestamp != null ? timestamp.hashCode() : 0);

        res = 31 * res + (int)(activityId ^ (activityId >>> 32));

        res = 31 * res + (useraccountId != null ? useraccountId.hashCode() : 0);

        res = 31 * res + (int)(activitystateEnumid ^ (activitystateEnumid >>> 32));

        res = 31 * res + (remainingdurationunitEnumid != null ? remainingdurationunitEnumid.hashCode() : 0);

        res = 31 * res + (usercomment != null ? usercomment.hashCode() : 0);

        res = 31 * res + (hoursspent != null ? hoursspent.hashCode() : 0);

        res = 31 * res + (remainingduration != null ? remainingduration.hashCode() : 0);

        res = 31 * res + (int)sequencenr;

        res = 31 * res + (logdate != null ? logdate.hashCode() : 0);

        res = 31 * res + (sessionId != null ? sessionId.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Activityhistory [activityhistoryId=" + activityhistoryId +
            ", timestamp=" + timestamp +
            ", activityId=" + activityId +
            ", useraccountId=" + useraccountId +
            ", activitystateEnumid=" + activitystateEnumid +
            ", remainingdurationunitEnumid=" + remainingdurationunitEnumid +
            ", usercomment=" + usercomment +
            ", hoursspent=" + hoursspent +
            ", remainingduration=" + remainingduration +
            ", sequencenr=" + sequencenr +
            ", logdate=" + logdate +
            ", sessionId=" + sessionId +
            "]";
    }
}

