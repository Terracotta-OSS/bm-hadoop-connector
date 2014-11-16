package org.terracotta.bigmemory.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import net.sf.ehcache.Element;


import org.apache.hadoop.io.Writable;

/**
 * Writable wrapper for Ehcache Element types. 
 *
 */

public class BigmemoryElementWritable implements Writable {

  private byte[] elemBytes;
  protected Element element;


  /**
   * Creates a new BigmemoryElementWritable
   * @param key
   * @param value
   * @throws java.io.IOException
   */
  public BigmemoryElementWritable(Serializable key, Serializable value) throws IOException {
    this.element = new Element(key, value);
    ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
    ObjectOutputStream objOutStream = new ObjectOutputStream(byteArrayOutStream);
    objOutStream.writeObject(element);
    this.elemBytes = byteArrayOutStream.toByteArray();
  }


  /**
   * Serialize the fields of this object to <code>out</code>.
   *
   * @param dataOutput <code>DataOuput</code> to serialize this object into.
   * @throws java.io.IOException
   */
  public void write(DataOutput dataOutput) throws IOException {
    if (elemBytes != null) {
      dataOutput.writeInt(elemBytes.length);
      dataOutput.write(elemBytes);
    }

  }

  /**
   * Deserialize the fields of this object from <code>in</code>.
   * <p/>
   * <p>For efficiency, implementations should attempt to re-use storage in the
   * existing object where possible.</p>
   *
   * @param in <code>DataInput</code> to deserialize this object from.
   * @throws java.io.IOException
   */
  public void readFields(DataInput in) throws IOException {
    int elemSize = in.readInt();
    byte[] bytes = new byte[elemSize];
    in.readFully(bytes);
    try {
      this.element = toEhcacheElement(bytes);
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Can't convert input object to Ehcache Element");
    }

  }

  private Element toEhcacheElement (byte[] bytes) throws IOException, ClassNotFoundException {
    Element elem = null;
    ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
    ObjectInputStream ois = new ObjectInputStream (bis);
    elem = (Element)ois.readObject();
    return elem;
  }

  public Element getElement() {
    return element;
  }

}


