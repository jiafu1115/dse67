package org.apache.cassandra.index.sasi.analyzer.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterPipelineExecutor {
   private static final Logger logger = LoggerFactory.getLogger(FilterPipelineExecutor.class);

   public FilterPipelineExecutor() {
   }

   public static <F, T> T execute(final FilterPipelineTask<F, T> task, final T initialInput) {
      FilterPipelineTask<?, ?> taskPtr = task;
      T result = initialInput;
      try {
         do {
            final FilterPipelineTask<F, T> taskGeneric = (FilterPipelineTask<F, T>)taskPtr;
            result = taskGeneric.process((F)result);
            taskPtr = taskPtr.next;
         } while (taskPtr != null);
         return result;
      }
      catch (Exception e) {
         FilterPipelineExecutor.logger.info("An unhandled exception to occurred while processing pipeline [{}]", (Object)task.getName(), (Object)e);
         return null;
      }
   }
}
