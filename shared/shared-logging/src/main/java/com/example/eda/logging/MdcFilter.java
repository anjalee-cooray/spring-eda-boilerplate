package com.example.eda.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.lang.NonNull;
import org.springframework.web.filter.OncePerRequestFilter;

public class MdcFilter extends OncePerRequestFilter {

    static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    static final String TENANT_ID_HEADER = "X-Tenant-Id";

    static final String MDC_CORRELATION_ID = "correlation_id";
    static final String MDC_TENANT_ID = "tenant_id";
    static final String MDC_TRACE_ID = "trace_id";

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain chain) throws ServletException, IOException {

        try {
            String correlationId = request.getHeader(CORRELATION_ID_HEADER);
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
            }

            String tenantId = request.getHeader(TENANT_ID_HEADER);

            MDC.put(MDC_CORRELATION_ID, correlationId);
            MDC.put(MDC_TRACE_ID, correlationId);

            if (tenantId != null && !tenantId.isBlank()) {
                MDC.put(MDC_TENANT_ID, tenantId);
            }

            response.setHeader(CORRELATION_ID_HEADER, correlationId);

            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_CORRELATION_ID);
            MDC.remove(MDC_TENANT_ID);
            MDC.remove(MDC_TRACE_ID);
        }
    }
}
