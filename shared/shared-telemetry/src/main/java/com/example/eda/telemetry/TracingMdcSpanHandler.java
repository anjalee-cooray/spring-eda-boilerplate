package com.example.eda.telemetry;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import org.slf4j.MDC;
import org.springframework.lang.NonNull;

/**
 * Writes the active OTel/Brave trace ID into MDC so it appears in every log line
 * alongside the correlation_id set by MdcFilter.
 */
public class TracingMdcSpanHandler extends SpanHandler {

    static final String MDC_TRACE_ID = "trace_id";
    static final String MDC_SPAN_ID = "span_id";

    @Override
    public boolean begin(@NonNull TraceContext context, @NonNull MutableSpan span, TraceContext parent) {
        MDC.put(MDC_TRACE_ID, context.traceIdString());
        MDC.put(MDC_SPAN_ID, context.spanIdString());
        return true;
    }

    @Override
    public boolean end(@NonNull TraceContext context, @NonNull MutableSpan span, @NonNull Cause cause) {
        MDC.remove(MDC_TRACE_ID);
        MDC.remove(MDC_SPAN_ID);
        return true;
    }
}
