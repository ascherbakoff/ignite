/**
 * Copyright 2015 FSB Technology (UK) Ltd ("FSB") The contents of this file are the property of FSB. You may not use the
 * contents of this file without the express permission of FSB.
 */

import org.apache.ignite.cache.affinity.AffinityKey;

/**
 * Every class should extend from this class which has an Id associated with it.
 * 
 * @author luqman on 16 Nov 2015
 */
public abstract class FSBObjectWithId<T>
{
    private static final long serialVersionUID = 1L;

    protected T id;

    protected AffinityKey<T> key;

    /**
     * Need default constructor for myBatis
     */
    public FSBObjectWithId()
    {
        super();
    }

    public FSBObjectWithId(T id)
    {
        this.id = id;
        key();
    }

    public T getId()
    {
        if (key != null)
        {
            return key.key();
        }

        return id;
    }

    public void setId(T id)
    {
        this.id = id;
        key();
    }

    public AffinityKey<T> key()
    {
        Object affinityKey = affinityKey();

        if (affinityKey != null)
        {
            key = new AffinityKey<>(id, affinityKey);
        }

        return key;
    }

    protected Object affinityKey()
    {
        // Deliberately returning null;
        return null;
    }
}
