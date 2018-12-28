package com.datastax.bdp.tools;

import com.datastax.bdp.insights.storage.config.DuplicateFilteringRuleError;
import com.datastax.bdp.insights.storage.config.FilteringRule;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.plugin.InsightsConfigPluginMXBean;
import com.datastax.insights.core.json.JacksonUtil;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.management.MalformedObjectNameException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

public class InsightsFiltersCommands implements ProxySource {
   public InsightsFiltersCommands() {
   }

   public void makeProxies(NodeJmxProxyPool probe) throws MalformedObjectNameException {
      makeProxy(probe);
   }

   private static InsightsConfigPluginMXBean makeProxy(NodeJmxProxyPool probe) throws MalformedObjectNameException {
      return (InsightsConfigPluginMXBean)probe.makeProxy(JMX.Type.INSIGHTS, "InsightsRuntimeConfig", InsightsConfigPluginMXBean.class);
   }

   public static class ConfigureInsightsCommand extends DseTool.Plugin {
      private static final Options OPTIONS = (new Options()).addOption("show_filters", "show_filters", false, "Display the existing filtering rules").addOption("remove_all_filters", "remove_all_filters", false, "Remove all the existing filtering rules (allows all)").addOption("add", "add", false, "Add a filtering rule to the list").addOption("remove", "remove", false, "Remove a filtering rule from the list").addOption("allow", "allow", false, "Set the filtering rule type to 'allow': meaning, any name that matches this rule will be allowed through").addOption("deny", "deny", false, "Set the filtering rule type to 'deny': meaning, any name that matches this rule will be blocked").addOption("global", "global", false, "Set the filtering rule scope to 'global': meaning, rule affects insights and collectd reporting").addOption("insights_only", "insights_only", false, "Set the filtering rule scope to 'insights_only': meaning, rule affects insights reporting only and *not* collectd");

      public ConfigureInsightsCommand() {
      }

      public String getName() {
         return "insights_filters";
      }

      public boolean isJMX() {
         return true;
      }

      public String getHelp() {
         return "Insights Filtering Configuration";
      }

      public String getOptionsHelp() {
         return String.format("[simple commands]:\n  --%s\n  --%s\n[filtering actions]:\n  --%s\n  --%s\n  --%s\n  --%s\n  --%s\n  --%s\n <regex>\n\n[examples]:\n %s --%s --%s --%s .*KeyspaceMetrics.*\n %s --%s --%s --%s .*gc.*\n %s --%s\n", new Object[]{"show_filters", "remove_all_filters", "add", "remove", "allow", "deny", "global", "insights_only", "insights_filters", "add", "global", "deny", "insights_filters", "add", "insights_only", "deny", "insights_filters", "remove_all_filters"});
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, DseToolArgumentParser parser) throws Exception {
         CommandLineParser cliParser = new DefaultParser();
         CommandLine commandLine = cliParser.parse(OPTIONS, parser.getArguments());
         if(commandLine.getOptions().length == 0) {
            System.out.println(this.getOptionsHelp());
         } else {
            InsightsConfigPluginMXBean proxy = InsightsFiltersCommands.makeProxy(dnp);
            if(commandLine.hasOption("show_filters")) {
               if(commandLine.getArgList().size() <= 0 && commandLine.getOptions().length <= 1) {
                  System.out.println(proxy.getFilteringRules());
               } else {
                  throw new IllegalArgumentException("Error: show_filters must be run with no arguments");
               }
            } else if(commandLine.hasOption("remove_all_filters")) {
               if(commandLine.getArgList().size() <= 0 && commandLine.getOptions().length <= 1) {
                  proxy.removeAllFilteringRules();
                  System.out.println("Success: All filtering rules removed.");
               } else {
                  throw new IllegalArgumentException("Error: show_filters must be run with no arguments");
               }
            } else {
               Boolean isAdd = null;
               if(!commandLine.hasOption("add") && !commandLine.hasOption("remove")) {
                  throw new IllegalArgumentException(String.format("Error: '--%s' or '--%s' option required", new Object[]{"add", "remove"}));
               } else {
                  if(commandLine.hasOption("add")) {
                     isAdd = Boolean.valueOf(true);
                  }

                  if(commandLine.hasOption("remove")) {
                     if(isAdd != null) {
                        throw new IllegalArgumentException(String.format("Error: '--%s' and '--%s' cannot be both specified", new Object[]{"add", "remove"}));
                     }

                     isAdd = Boolean.valueOf(false);
                  }

                  if(isAdd == null) {
                     isAdd = Boolean.valueOf(false);
                  }

                  String scope = null;
                  if(!commandLine.hasOption("global") && !commandLine.hasOption("insights_only")) {
                     throw new IllegalArgumentException(String.format("Error: '--%s' or '--%s' option required", new Object[]{"global", "insights_only"}));
                  } else {
                     if(commandLine.hasOption("global")) {
                        scope = "global";
                     }

                     if(commandLine.hasOption("insights_only")) {
                        if(scope != null) {
                           throw new IllegalArgumentException(String.format("Error: '--%s' and '--%s' cannot be both specified", new Object[]{"global", "insights_only"}));
                        }

                        scope = "insights";
                     }

                     String policy = null;
                     if(!commandLine.hasOption("allow") && !commandLine.hasOption("deny")) {
                        throw new IllegalArgumentException(String.format("Error: '--%s' or '--%s' option required", new Object[]{"allow", "deny"}));
                     } else {
                        if(commandLine.hasOption("allow")) {
                           policy = "allow";
                        }

                        if(commandLine.hasOption("deny")) {
                           if(policy != null) {
                              throw new IllegalArgumentException(String.format("Error: '--%s' and '--%s' cannot be both specified", new Object[]{"allow", "deny"}));
                           }

                           policy = "deny";
                        }

                        if(commandLine.getArgs().length == 0) {
                           throw new IllegalArgumentException("Error: regular expression required after rule options");
                        } else if(commandLine.getArgs().length > 1) {
                           throw new IllegalArgumentException("Error: extra arguments found after " + commandLine.getArgs()[0]);
                        } else {
                           String regex = commandLine.getArgs()[0];
                           System.out.println("Regex = " + regex);

                           try {
                              Pattern.compile(regex);
                           } catch (PatternSyntaxException var14) {
                              throw new IllegalArgumentException("Error parsing regular expression " + regex + ": " + var14.getDescription());
                           }

                           FilteringRule rule = new FilteringRule(policy, regex, scope);
                           if(isAdd.booleanValue()) {
                              try {
                                 proxy.addFilteringRule(JacksonUtil.writeValueAsString(rule));
                              } catch (DuplicateFilteringRuleError var13) {
                                 System.out.println(var13.getMessage());
                                 return;
                              }
                           } else {
                              proxy.removeFilteringRule(JacksonUtil.writeValueAsString(rule));
                           }

                           System.out.println(String.format("Successfully %s rule %s insight filtering rule list", new Object[]{isAdd.booleanValue()?"added":"removed", isAdd.booleanValue()?"to":"from"}));
                        }
                     }
                  }
               }
            }
         }
      }
   }
}
