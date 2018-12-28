package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class ValidationComplete extends RepairMessage<ValidationComplete> {
   public static Versioned<RepairVerbs.RepairVersion, RepairMessage.MessageSerializer<ValidationComplete>> serializers = RepairVerbs.RepairVersion.versioned((v) -> {
      return new RepairMessage.MessageSerializer<ValidationComplete>(v) {
         private final MerkleTrees.MerkleTreesSerializer merkleTreesSerializer;
         private final Serializer repairJobDescSerializers;

         {
            this.repairJobDescSerializers = (Serializer)RepairJobDesc.serializers.get(v);
            this.merkleTreesSerializer = (MerkleTrees.MerkleTreesSerializer)MerkleTrees.serializers.get(this.version);
         }

         public void serialize(ValidationComplete message, DataOutputPlus out) throws IOException {
            this.repairJobDescSerializers.serialize(message.desc, out);
            out.writeBoolean(message.success());
            if(message.trees != null) {
               this.merkleTreesSerializer.serialize(message.trees, out);
            }

         }

         public ValidationComplete deserialize(DataInputPlus in) throws IOException {
            RepairJobDesc desc = (RepairJobDesc)this.repairJobDescSerializers.deserialize(in);
            boolean success = in.readBoolean();
            if(success) {
               MerkleTrees trees = this.merkleTreesSerializer.deserialize(in);
               return new ValidationComplete(desc, trees);
            } else {
               return new ValidationComplete(desc);
            }
         }

         public long serializedSize(ValidationComplete message) {
            long size = this.repairJobDescSerializers.serializedSize(message.desc);
            size += (long)TypeSizes.sizeof(message.success());
            if(message.trees != null) {
               size += this.merkleTreesSerializer.serializedSize(message.trees);
            }

            return size;
         }
      };
   });
   public final MerkleTrees trees;

   public ValidationComplete(RepairJobDesc desc) {
      super(desc);
      this.trees = null;
   }

   public RepairMessage.MessageSerializer<ValidationComplete> serializer(RepairVerbs.RepairVersion version) {
      return (RepairMessage.MessageSerializer)serializers.get(version);
   }

   public Optional<Verb<ValidationComplete, ?>> verb() {
      return Optional.of(Verbs.REPAIR.VALIDATION_COMPLETE);
   }

   public ValidationComplete(RepairJobDesc desc, MerkleTrees trees) {
      super(desc);

      assert trees != null;

      this.trees = trees;
   }

   public boolean success() {
      return this.trees != null;
   }

   public boolean equals(Object o) {
      if(!(o instanceof ValidationComplete)) {
         return false;
      } else {
         ValidationComplete other = (ValidationComplete)o;
         return this.desc.equals(other.desc);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.desc});
   }
}
