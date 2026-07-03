package com.example.eda.db.rls;

import com.example.eda.security.TenantContextHolder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * Sets the PostgreSQL session variable app.tenant_id before every repository call,
 * activating the RLS policy on all tenant-scoped tables.
 */
@Aspect
@Component
public class RlsDataSourceInterceptor {

    private final DataSource dataSource;

    public RlsDataSourceInterceptor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Around("execution(* org.springframework.data.repository.Repository+.*(..))")
    public Object setTenantId(ProceedingJoinPoint joinPoint) throws Throwable {
        String tenantId = resolveTenantId();
        if (tenantId != null) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement("SET LOCAL app.tenant_id = ?")) {
                stmt.setString(1, tenantId);
                stmt.execute();
            } catch (SQLException e) {
                throw new RlsException("Failed to set RLS tenant context", e);
            }
        }
        return joinPoint.proceed();
    }

    private String resolveTenantId() {
        var ctx = TenantContextHolder.getOrNull();
        return ctx != null ? ctx.tenantId() : null;
    }
}
