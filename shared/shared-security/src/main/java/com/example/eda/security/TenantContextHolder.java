package com.example.eda.security;

public final class TenantContextHolder {

    private static final ThreadLocal<TenantContext> CONTEXT = new ThreadLocal<>();

    private TenantContextHolder() {
    }

    public static void set(TenantContext context) {
        CONTEXT.set(context);
    }

    public static TenantContext get() {
        TenantContext ctx = CONTEXT.get();
        if (ctx == null) {
            throw new IllegalStateException("No TenantContext bound to current thread");
        }
        return ctx;
    }

    public static TenantContext getOrNull() {
        return CONTEXT.get();
    }

    public static void clear() {
        CONTEXT.remove();
    }
}
