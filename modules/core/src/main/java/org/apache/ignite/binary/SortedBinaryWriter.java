package org.apache.ignite.binary;

/**
 * <p> The <code>SortedBinaryWriter</code> </p>
 *
 * @author Alexei Scherbakov
 */
public interface SortedBinaryWriter {
    public void writeInt(String fieldName, int val) throws BinaryObjectException;
}
