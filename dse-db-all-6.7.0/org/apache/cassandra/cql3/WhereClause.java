package org.apache.cassandra.cql3;

import java.util.List;
import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;
import org.apache.cassandra.utils.UnmodifiableArrayList;

public class WhereClause {
   private static final WhereClause EMPTY = new WhereClause(new WhereClause.Builder());
   public final List<Relation> relations;
   public final List<CustomIndexExpression> expressions;

   protected WhereClause(WhereClause.Builder builder) {
      this.relations = builder.relations.build();
      this.expressions = builder.expressions.build();
   }

   public static WhereClause empty() {
      return EMPTY;
   }

   public boolean containsCustomExpressions() {
      return !this.expressions.isEmpty();
   }

   public static final class Builder {
      UnmodifiableArrayList.Builder<Relation> relations = UnmodifiableArrayList.builder();
      UnmodifiableArrayList.Builder<CustomIndexExpression> expressions = UnmodifiableArrayList.builder();

      public Builder() {
      }

      public WhereClause.Builder add(Relation relation) {
         this.relations.add(relation);
         return this;
      }

      public WhereClause.Builder add(CustomIndexExpression expression) {
         this.expressions.add(expression);
         return this;
      }

      public WhereClause build() {
         return new WhereClause(this);
      }
   }
}
