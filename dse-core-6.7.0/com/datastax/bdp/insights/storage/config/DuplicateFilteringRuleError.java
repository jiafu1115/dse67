package com.datastax.bdp.insights.storage.config;

public class DuplicateFilteringRuleError extends Error {
   public DuplicateFilteringRuleError(FilteringRule duplicate) {
      super(String.format("A rule already exists with the pattern %s", new Object[]{duplicate.patternStr}));
   }
}
