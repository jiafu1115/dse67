package com.datastax.bdp.insights.storage.credentials;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.config.InsightsConfig;
import com.datastax.bdp.insights.events.InsightClusterFingerprint;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.datastax.insights.client.InsightsClient;
import com.datastax.insights.client.TokenStore;
import com.google.common.base.Suppliers;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.Wrapped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public final class DseInsightsTokenStore implements TokenStore {
   private static final Logger logger = LoggerFactory.getLogger(DseInsightsTokenStore.class);
   private static final Supplier<File> tokenFile;
   private static final String TOKENS_TABLE;
   private static final String HOST_COLUMN = "node";
   private static final String TOKEN_COLUMN = "bearer_token";
   private static final String LAST_UPDATED = "last_updated";
   private static final String MAX_ADDED_DATE_SEEN_BY_NODE = "max_added_date_seen_by_node";
   private static final String INSERT_TOKEN_CQL;
   private static final String UPDATE_TOKEN_CQL;
   private static final String SELECT_ALL_TOKEN_CQL;
   private final Wrapped<Optional<String>> cachedToken = Wrapped.create(Optional.empty());
   private final Wrapped<List<InsightClusterFingerprint.Fingerprint>> cachedFingerprint;

   @Inject
   public DseInsightsTokenStore() {
      this.cachedFingerprint = Wrapped.create(Collections.EMPTY_LIST);
   }

   public boolean isEmpty() {
      try {
         return this.maybeGetTokenFromDisk().isPresent();
      } catch (Exception var2) {
         logger.warn("Exception attempting to locate insights token in storage", var2);
         return true;
      }
   }

   public Optional<String> token() {
      return this.maybeGetTokenFromDisk();
   }

   public boolean store(String token) {
      assert token != null;

      try {
         Optional<String> opToken = Optional.of(token);
         if(this.maybeGetTokenFromDisk().isPresent()) {
            this.updateTokenInCQL(opToken);
         } else {
            this.storeTokenInCQL(opToken, new Date());
         }

         this.storeTokenOnDisk(opToken);
         this.cachedToken.set(opToken);
         return true;
      } catch (Exception var3) {
         logger.error("Error attempting to store insights credentials", var3);
         return false;
      }
   }

   private void storeTokenOnDisk(Optional<String> replacementToken) throws IOException {
      if(!replacementToken.isPresent()) {
         throw new IllegalStateException("Cannot insert empty credentials!");
      } else {
         FileWriter writer = new FileWriter((File)tokenFile.get());
         Throwable var3 = null;

         try {
            writer.write((String)replacementToken.get());
         } catch (Throwable var12) {
            var3 = var12;
            throw var12;
         } finally {
            if(writer != null) {
               if(var3 != null) {
                  try {
                     writer.close();
                  } catch (Throwable var11) {
                     var3.addSuppressed(var11);
                  }
               } else {
                  writer.close();
               }
            }

         }

         logger.debug("Stored insights token to {}", tokenFile.get());
      }
   }

   public synchronized UntypedResultSet updateTokenInCQL(Optional<String> replacementToken) {
      if(!replacementToken.isPresent()) {
         throw new IllegalStateException("Cannot insert empty credentials!");
      } else {
         return QueryProcessorUtil.execute(UPDATE_TOKEN_CQL, ConsistencyLevel.LOCAL_ONE, new Object[]{replacementToken.get(), new Date(), SystemInfo.getHostId()});
      }
   }

   public synchronized UntypedResultSet storeTokenInCQL(Optional<String> token, Date maxAddedDateSeenByNodes) {
      if(!token.isPresent()) {
         throw new IllegalStateException("Cannot insert empty credentials!");
      } else if(maxAddedDateSeenByNodes == null) {
         throw new IllegalStateException("Cannot insert empty added date!");
      } else {
         return QueryProcessorUtil.execute(INSERT_TOKEN_CQL, ConsistencyLevel.LOCAL_ONE, new Object[]{SystemInfo.getHostId(), token.get(), maxAddedDateSeenByNodes, new Date()});
      }
   }

   private UntypedResultSet selectAllTokenFromCQL() {
      SelectStatement selectStatement = (SelectStatement)StatementUtils.prepareStatementBlocking(SELECT_ALL_TOKEN_CQL, QueryState.forInternalCalls(), "Error preparing \"" + SELECT_ALL_TOKEN_CQL + "\"");
      return QueryProcessorUtil.processPreparedSelect(selectStatement, ConsistencyLevel.LOCAL_ONE);
   }

   private Optional<String> maybeGetTokenFromDisk() {
      if(!((Optional)this.cachedToken.get()).isPresent() && ((File)tokenFile.get()).exists()) {
         try {
            String token = Files.readFirstLine((File)tokenFile.get(), Charset.defaultCharset());
            token = token.trim();
            if(!token.isEmpty()) {
               this.cachedToken.set(Optional.of(token));
            }
         } catch (IOException var2) {
            logger.warn("Error reading token file {}", tokenFile.get(), var2);
         }
      } else {
         logger.trace("Token file missing {}", tokenFile.get());
      }

      return (Optional)this.cachedToken.get();
   }

   public void checkFingerprint(InsightsClient client) throws Exception {
      Optional<String> token = this.token();
      if(!token.isPresent()) {
         logger.trace("No token found");
      } else {
         UntypedResultSet resultSet = this.selectAllTokenFromCQL();
         InsightClusterFingerprint.Fingerprint self = null;
         Date maxAddedDate = null;
         List<InsightClusterFingerprint.Fingerprint> fingerprintList = new ArrayList();
         Iterator var7 = resultSet.iterator();

         while(var7.hasNext()) {
            Row row = (Row)var7.next();
            UUID host = row.getUUID("node");
            Date maxAddedSeenByNode = row.getTimestamp("max_added_date_seen_by_node");
            Date updated = row.getTimestamp("last_updated");
            String aToken = row.getString("bearer_token");
            InsightClusterFingerprint.Fingerprint fingerprint = new InsightClusterFingerprint.Fingerprint(host, maxAddedSeenByNode, updated, aToken);
            if(host.equals(SystemInfo.getHostId())) {
               self = fingerprint;
            }

            fingerprintList.add(fingerprint);
            if(maxAddedDate == null || fingerprint.maxAddedDateSeenByNode != null && fingerprint.maxAddedDateSeenByNode.after(maxAddedDate)) {
               maxAddedDate = fingerprint.maxAddedDateSeenByNode;
            }
         }

         if(self != null && fingerprintList.size() > 1 && self.maxAddedDateSeenByNode != null) {
            if(maxAddedDate.after(self.maxAddedDateSeenByNode)) {
               this.storeTokenInCQL(this.maybeGetTokenFromDisk(), maxAddedDate);
               logger.trace("Waiting for next interval {}", fingerprintList);
            } else {
               if(!Objects.equals(this.cachedFingerprint.get(), fingerprintList)) {
                  List<InsightClusterFingerprint.Fingerprint> filteredList = new ArrayList(fingerprintList.size());
                  Iterator var15 = fingerprintList.iterator();

                  while(var15.hasNext()) {
                     InsightClusterFingerprint.Fingerprint fingerprint = (InsightClusterFingerprint.Fingerprint)var15.next();
                     if(fingerprint.maxAddedDateSeenByNode.equals(maxAddedDate)) {
                        filteredList.add(fingerprint);
                     }
                  }

                  if(client.report(new InsightClusterFingerprint(filteredList))) {
                     this.cachedFingerprint.set(filteredList);
                  }
               }

            }
         } else {
            logger.trace("Waiting for results {}", fingerprintList);
         }
      }
   }

   static {
      com.google.common.base.Supplier var10000 = Suppliers.memoize(() -> {
         return new File((String)InsightsConfig.logDirectory.get() + File.separator + "insights.token");
      });
      tokenFile = var10000::get;
      TOKENS_TABLE = String.format("%s.%s", new Object[]{"dse_insights", "tokens"});
      INSERT_TOKEN_CQL = String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)", new Object[]{TOKENS_TABLE, "node", "bearer_token", "max_added_date_seen_by_node", "last_updated"});
      UPDATE_TOKEN_CQL = String.format("UPDATE %s SET %s = ?, %s = ? WHERE %s = ?", new Object[]{TOKENS_TABLE, "bearer_token", "last_updated", "node"});
      SELECT_ALL_TOKEN_CQL = String.format("SELECT * FROM %s", new Object[]{TOKENS_TABLE});
   }
}
