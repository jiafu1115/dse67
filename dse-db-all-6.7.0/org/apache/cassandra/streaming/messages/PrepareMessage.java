package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamRequest;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.Serializer;

public class PrepareMessage extends StreamMessage {
   public static StreamMessage.Serializer<PrepareMessage> serializer = new StreamMessage.Serializer<PrepareMessage>() {
      public PrepareMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         DataInputPlus input = new DataInputPlus.DataInputStreamPlus(Channels.newInputStream(in));
         PrepareMessage message = new PrepareMessage();
         Serializer<StreamRequest> streamRequestSerializer = (Serializer)StreamRequest.serializers.get(version);
         int numRequests = input.readInt();

         for(int i = 0; i < numRequests; ++i) {
            message.requests.add(streamRequestSerializer.deserialize(input));
         }

         Serializer<StreamSummary> streamSummarySerializer = (Serializer)StreamSummary.serializers.get(version);
         int numSummaries = input.readInt();

         for(int ix = 0; ix < numSummaries; ++ix) {
            message.summaries.add(streamSummarySerializer.deserialize(input));
         }

         return message;
      }

      public void serialize(PrepareMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         Serializer<StreamRequest> streamRequestSerializer = (Serializer)StreamRequest.serializers.get(version);
         out.writeInt(message.requests.size());
         Iterator var6 = message.requests.iterator();

         while(var6.hasNext()) {
            StreamRequest request = (StreamRequest)var6.next();
            streamRequestSerializer.serialize(request, out);
         }

         Serializer<StreamSummary> streamSummarySerializer = (Serializer)StreamSummary.serializers.get(version);
         out.writeInt(message.summaries.size());
         Iterator var10 = message.summaries.iterator();

         while(var10.hasNext()) {
            StreamSummary summary = (StreamSummary)var10.next();
            streamSummarySerializer.serialize(summary, out);
         }

      }
   };
   public final Collection<StreamRequest> requests = new ArrayList();
   public final Collection<StreamSummary> summaries = new ArrayList();

   public PrepareMessage() {
      super(StreamMessage.Type.PREPARE);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("Prepare (");
      sb.append(this.requests.size()).append(" requests:[");
      boolean first = true;

      StreamRequest request;
      for(Iterator var3 = this.requests.iterator(); var3.hasNext(); sb.append(request)) {
         request = (StreamRequest)var3.next();
         if(first) {
            first = false;
         } else {
            sb.append(',');
         }
      }

      sb.append("], ");
      int totalFile = 0;
      long totalSize = 0L;

      Iterator var6;
      StreamSummary summary;
      for(var6 = this.summaries.iterator(); var6.hasNext(); totalSize += summary.totalSize) {
         summary = (StreamSummary)var6.next();
         totalFile += summary.files;
      }

      sb.append(" ").append(totalFile).append(" files, ");
      sb.append(" ").append(totalSize).append(" bytes, summaries:[");
      first = true;

      for(var6 = this.summaries.iterator(); var6.hasNext(); sb.append(summary.toString())) {
         summary = (StreamSummary)var6.next();
         if(first) {
            first = false;
         } else {
            sb.append(',');
         }
      }

      sb.append("]}");
      return sb.toString();
   }
}
