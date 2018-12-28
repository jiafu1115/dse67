package com.datastax.bdp.insights.events;

import com.datastax.insights.core.Insight;
import com.datastax.insights.core.InsightMetadata;
import com.datastax.insights.core.InsightType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.cassandra.utils.time.ApproximateTime;

public class ExceptionInformation extends Insight {
   public static final String NAME = "dse.insights.event.exception";

   public ExceptionInformation(Throwable t) {
      super(new InsightMetadata("dse.insights.event.exception", Optional.of(Long.valueOf(ApproximateTime.millisTime())), Optional.empty(), Optional.of(InsightType.EVENT), Optional.empty()), new ExceptionInformation.Data(t));
   }

   public static class InnerData {
      @JsonProperty("exception_class")
      public final String exceptionClass;
      @JsonProperty("stack_trace")
      public final List<StackTraceElement> stackTrace;

      InnerData(Throwable t) {
         this.exceptionClass = t.getClass().getCanonicalName();
         this.stackTrace = Lists.newArrayList(t.getStackTrace());
      }
   }

   public static class Data {
      @JsonProperty("exception_causes")
      public final List<ExceptionInformation.InnerData> causes;

      Data(Throwable t) {
         for(this.causes = new ArrayList(); t != null; t = t.getCause()) {
            this.causes.add(new ExceptionInformation.InnerData(t));
         }

      }
   }
}
