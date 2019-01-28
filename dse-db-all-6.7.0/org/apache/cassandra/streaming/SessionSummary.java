package org.apache.cassandra.streaming;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class SessionSummary implements Serializable {
   public static final Versioned<RepairVerbs.RepairVersion, Serializer<SessionSummary>> serializers = RepairVerbs.RepairVersion.versioned(SessionSummary.SessionSummarySerializer::new);
   public final InetAddress coordinator;
   public final InetAddress peer;
   public final Collection<StreamSummary> receivingSummaries;
   public final Collection<StreamSummary> sendingSummaries;

   public SessionSummary(InetAddress coordinator, InetAddress peer, Collection<StreamSummary> receivingSummaries, Collection<StreamSummary> sendingSummaries) {
      assert coordinator != null;

      assert peer != null;

      assert receivingSummaries != null;

      assert sendingSummaries != null;

      this.coordinator = coordinator;
      this.peer = peer;
      this.receivingSummaries = receivingSummaries;
      this.sendingSummaries = sendingSummaries;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         SessionSummary summary = (SessionSummary)o;
         return !this.coordinator.equals(summary.coordinator)?false:(!this.peer.equals(summary.peer)?false:(!this.receivingSummaries.equals(summary.receivingSummaries)?false:this.sendingSummaries.equals(summary.sendingSummaries)));
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.coordinator.hashCode();
      result = 31 * result + this.peer.hashCode();
      result = 31 * result + this.receivingSummaries.hashCode();
      result = 31 * result + this.sendingSummaries.hashCode();
      return result;
   }

   public static class SessionSummarySerializer extends VersionDependent<RepairVerbs.RepairVersion> implements Serializer<SessionSummary> {
      private final Serializer<StreamSummary> streamSummarySerializer;

      protected SessionSummarySerializer(RepairVerbs.RepairVersion version) {
         super(version);
         this.streamSummarySerializer = (Serializer)StreamSummary.serializers.get(version.streamVersion);
      }

      public void serialize(SessionSummary summary, DataOutputPlus out) throws IOException {
         ByteBufferUtil.writeWithLength(InetAddressSerializer.instance.serialize(summary.coordinator), out);
         ByteBufferUtil.writeWithLength(InetAddressSerializer.instance.serialize(summary.peer), out);
         out.writeInt(summary.receivingSummaries.size());
         Iterator var3 = summary.receivingSummaries.iterator();

         StreamSummary streamSummary;
         while(var3.hasNext()) {
            streamSummary = (StreamSummary)var3.next();
            this.streamSummarySerializer.serialize(streamSummary, out);
         }

         out.writeInt(summary.sendingSummaries.size());
         var3 = summary.sendingSummaries.iterator();

         while(var3.hasNext()) {
            streamSummary = (StreamSummary)var3.next();
            this.streamSummarySerializer.serialize(streamSummary, out);
         }

      }

      public SessionSummary deserialize(DataInputPlus in) throws IOException {
         InetAddress coordinator = InetAddressSerializer.instance.deserialize(ByteBufferUtil.readWithLength(in));
         InetAddress peer = InetAddressSerializer.instance.deserialize(ByteBufferUtil.readWithLength(in));
         int numRcvd = in.readInt();
         List<StreamSummary> receivingSummaries = new ArrayList(numRcvd);

         int numSent;
         for(numSent = 0; numSent < numRcvd; ++numSent) {
            receivingSummaries.add(this.streamSummarySerializer.deserialize(in));
         }

         numSent = in.readInt();
         List<StreamSummary> sendingSummaries = new ArrayList(numRcvd);

         for(int i = 0; i < numSent; ++i) {
            sendingSummaries.add(this.streamSummarySerializer.deserialize(in));
         }

         return new SessionSummary(coordinator, peer, receivingSummaries, sendingSummaries);
      }

      public long serializedSize(SessionSummary summary) {
         long size = 0L;
         size += (long)ByteBufferUtil.serializedSizeWithLength(InetAddressSerializer.instance.serialize(summary.coordinator));
         size += (long)ByteBufferUtil.serializedSizeWithLength(InetAddressSerializer.instance.serialize(summary.peer));
         size += (long)TypeSizes.sizeof(summary.receivingSummaries.size());

         Iterator var4;
         StreamSummary streamSummary;
         for(var4 = summary.receivingSummaries.iterator(); var4.hasNext(); size += this.streamSummarySerializer.serializedSize(streamSummary)) {
            streamSummary = (StreamSummary)var4.next();
         }

         size += (long)TypeSizes.sizeof(summary.sendingSummaries.size());

         for(var4 = summary.sendingSummaries.iterator(); var4.hasNext(); size += this.streamSummarySerializer.serializedSize(streamSummary)) {
            streamSummary = (StreamSummary)var4.next();
         }

         return size;
      }
   }
}
