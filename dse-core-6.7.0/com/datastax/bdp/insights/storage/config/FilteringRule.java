package com.datastax.bdp.insights.storage.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class FilteringRule {
   public static final String ALLOW = "allow";
   public static final String DENY = "deny";
   public static final String GLOBAL = "global";
   public static final String INSIGHT_ONLY = "insights";
   public static final FilteringRule FILTERED_GLOBALLY = new FilteringRule("deny", ".*", "global");
   public static final FilteringRule FILTERED_INSIGHTS = new FilteringRule("deny", ".*", "insights");
   public static final FilteringRule ALLOWED_GLOBALLY = new FilteringRule("allow", ".*", "global");
   public static final FilteringRule ALLOWED_INSIGHTS = new FilteringRule("allow", ".*", "insights");
   @JsonProperty("policy")
   public final String policy;
   @JsonProperty("pattern")
   public final String patternStr;
   @JsonProperty("scope")
   public final String scope;
   @JsonIgnore
   private final Supplier<Pattern> pattern;
   @JsonIgnore
   public final boolean isAllowRule;
   @JsonIgnore
   public final boolean isGlobal;

   @JsonCreator
   public FilteringRule(@JsonProperty("policy") String policy, @JsonProperty("pattern") String patternStr, @JsonProperty("scope") String scope) {
      if(!policy.equalsIgnoreCase("allow") && !policy.equalsIgnoreCase("deny")) {
         throw new IllegalArgumentException(String.format("Policy must be '%s' or '%s'", new Object[]{"allow", "deny"}));
      } else if(!scope.equalsIgnoreCase("global") && !scope.equalsIgnoreCase("insights")) {
         throw new IllegalArgumentException(String.format("Scope must be '%s' or '%s'", new Object[]{"global", "insights"}));
      } else {
         this.scope = scope;
         this.isGlobal = scope.equalsIgnoreCase("global");
         this.policy = policy;
         this.isAllowRule = policy.equalsIgnoreCase("allow");
         this.patternStr = patternStr;
         this.pattern = Suppliers.memoize(() -> {
            try {
               return Pattern.compile(patternStr, 2);
            } catch (PatternSyntaxException var2) {
               throw new IllegalArgumentException("Invalid pattern: " + patternStr, var2);
            }
         });
      }
   }

   public boolean isAllowed(String name) {
      boolean match = ((Pattern)this.pattern.get()).matcher(name).find();
      return this.isAllowRule?match:!match;
   }

   public static FilteringRule applyFilters(String name, Collection<FilteringRule> rules) {
      FilteringRule allowRule = null;
      FilteringRule denyRule = null;
      boolean hasDenyRule = false;
      boolean hasAllowRule = false;
      Iterator var6 = rules.iterator();

      while(true) {
         FilteringRule rule;
         label51:
         do {
            while(var6.hasNext()) {
               rule = (FilteringRule)var6.next();
               if(rule.isAllowRule) {
                  hasAllowRule = true;
                  continue label51;
               }

               hasDenyRule = true;
               if((denyRule == null || !denyRule.isGlobal) && !rule.isAllowed(name)) {
                  denyRule = rule;
               }
            }

            if(rules.isEmpty()) {
               return ALLOWED_GLOBALLY;
            }

            if(denyRule != null) {
               return denyRule;
            }

            if(allowRule != null) {
               return allowRule;
            }

            if(hasDenyRule && !hasAllowRule) {
               return ALLOWED_GLOBALLY;
            }

            return FILTERED_GLOBALLY;
         } while(allowRule != null && allowRule.isGlobal);

         if(rule.isAllowed(name)) {
            allowRule = rule;
         }
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         FilteringRule rule = (FilteringRule)o;
         return Objects.equals(this.policy, rule.policy) && Objects.equals(this.patternStr, rule.patternStr) && Objects.equals(this.scope, rule.scope);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.policy, this.patternStr, this.scope});
   }

   public String toString() {
      return "FilteringRule{policy='" + this.policy + '\'' + ", patternStr='" + this.patternStr + '\'' + ", scope='" + this.scope + '\'' + '}';
   }
}
