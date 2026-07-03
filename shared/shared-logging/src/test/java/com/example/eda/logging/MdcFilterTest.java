package com.example.eda.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MdcFilterTest {

    private final MdcFilter filter = new MdcFilter();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void populatesMdcFromRequestHeaders() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(MdcFilter.CORRELATION_ID_HEADER, "test-correlation-id");
        request.addHeader(MdcFilter.TENANT_ID_HEADER, "tenant-abc");

        MockHttpServletResponse response = new MockHttpServletResponse();

        FilterChain chain = (req, res) -> {
            assertThat(MDC.get(MdcFilter.MDC_CORRELATION_ID)).isEqualTo("test-correlation-id");
            assertThat(MDC.get(MdcFilter.MDC_TENANT_ID)).isEqualTo("tenant-abc");
            assertThat(MDC.get(MdcFilter.MDC_TRACE_ID)).isEqualTo("test-correlation-id");
        };

        filter.doFilterInternal(request, response, chain);
    }

    @Test
    void generatesCorrelationIdWhenHeaderAbsent() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        FilterChain chain = (req, res) ->
            assertThat(MDC.get(MdcFilter.MDC_CORRELATION_ID)).isNotBlank();

        filter.doFilterInternal(request, response, chain);
    }

    @Test
    void clearsMdcAfterRequest() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(MdcFilter.TENANT_ID_HEADER, "tenant-abc");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilterInternal(request, response, (req, res) -> { });

        assertThat(MDC.get(MdcFilter.MDC_TENANT_ID)).isNull();
        assertThat(MDC.get(MdcFilter.MDC_CORRELATION_ID)).isNull();
    }

    @Test
    void propagatesCorrelationIdInResponseHeader() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(MdcFilter.CORRELATION_ID_HEADER, "echo-me");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilterInternal(request, response, (req, res) -> { });

        assertThat(response.getHeader(MdcFilter.CORRELATION_ID_HEADER)).isEqualTo("echo-me");
    }
}
