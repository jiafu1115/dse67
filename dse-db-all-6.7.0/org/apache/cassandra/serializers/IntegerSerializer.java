package org.apache.cassandra.serializers;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IntegerSerializer implements TypeSerializer<BigInteger> {
   public static final IntegerSerializer instance = new IntegerSerializer();

   public IntegerSerializer() {
   }

   public BigInteger deserialize(ByteBuffer bytes) {
      return bytes.hasRemaining()?new BigInteger(ByteBufferUtil.getArray(bytes)):null;
   }

   public ByteBuffer serialize(BigInteger value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBuffer.wrap(value.toByteArray());
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
   }

   public String toString(BigInteger value) {
      return value == null?"":value.toString(10);
   }

   public Class<BigInteger> getType() {
      return BigInteger.class;
   }
}
