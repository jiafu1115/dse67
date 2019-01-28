package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.enums.PartitionedEnum;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class GrantPermissionsStatement extends PermissionsManagementStatement {
   public GrantPermissionsStatement(Set<Permission> permissions, IResource resource, RoleName grantee, GrantMode grantMode) {
      super(permissions, resource, grantee, grantMode);
   }

   protected String operation() {
      return this.grantMode.grantOperationName();
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.GRANT;
   }


   public Single<ResultMessage> execute(QueryState state) throws RequestValidationException, RequestExecutionException {
      return Single.fromCallable(() -> {
         IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
         Set<Permission> granted = authorizer.grant(state.getUser(), this.permissions, this.resource, this.grantee, new GrantMode[]{this.grantMode});
         if(!granted.equals(this.permissions) && !this.permissions.equals(authorizer.applicablePermissions(this.resource))) {
            String permissionsStr = (String)(new TreeSet<Permission>(this.permissions)).stream().filter((permission) -> {
               return !granted.contains(permission);
            }).map(PartitionedEnum::name).collect(Collectors.joining(", "));
            ClientWarn.instance.warn(this.grantMode.grantWarningMessage(this.grantee.getRoleName(), this.resource, permissionsStr));
         }

         return new ResultMessage.Void();
      });
   }

}
