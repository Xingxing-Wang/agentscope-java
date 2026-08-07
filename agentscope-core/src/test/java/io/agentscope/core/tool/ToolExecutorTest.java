/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.core.tool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.Event;
import io.agentscope.core.agent.StreamOptions;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.tool.test.SampleTools;
import io.agentscope.core.tool.test.ToolTestUtils;
import io.agentscope.core.util.JsonUtils;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Unit tests for ToolExecutor.
 *
 * <p>These tests verify execution paths that invoke the executor itself so regressions in
 * scheduling, ordering, timeout handling, and error propagation are detected.
 */
@Tag("unit")
@DisplayName("ToolExecutor Unit Tests")
class ToolExecutorTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private Toolkit toolkit;

    @BeforeEach
    void setUp() {
        toolkit = new Toolkit();
        toolkit.registerTool(new SampleTools());
    }

    @Test
    @DisplayName("Should execute multiple tool calls in parallel via Toolkit")
    void shouldExecuteToolsInParallel() {
        Map<String, Object> addInput = Map.of("a", 10, "b", 20);
        Map<String, Object> concatInput = Map.of("str1", "Hello", "str2", "World");

        ToolUseBlock addCall =
                ToolUseBlock.builder()
                        .id("call-add")
                        .name("add")
                        .input(addInput)
                        .content(JsonUtils.getJsonCodec().toJson(addInput))
                        .build();
        ToolUseBlock concatCall =
                ToolUseBlock.builder()
                        .id("call-concat")
                        .name("concat")
                        .input(concatInput)
                        .content(JsonUtils.getJsonCodec().toJson(concatInput))
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(addCall, concatCall), null, null, null).block(TIMEOUT);

        assertNotNull(responses, "Executor should return responses for tool calls");
        assertEquals(2, responses.size(), "All tool calls should be executed");

        Map<String, ToolResultBlock> responsesById =
                responses.stream()
                        .collect(Collectors.toMap(ToolResultBlock::getId, Function.identity()));

        ToolResultBlock addResponse = responsesById.get("call-add");
        ToolResultBlock concatResponse = responsesById.get("call-concat");

        assertNotNull(addResponse, "Add tool response should be present");
        assertEquals("30", extractFirstText(addResponse), "Add tool result mismatch");

        assertNotNull(concatResponse, "Concat tool response should be present");
        assertEquals(
                "\"HelloWorld\"", extractFirstText(concatResponse), "Concat tool result mismatch");
    }

    @Test
    @DisplayName("Should return explicit malformed-call error for invalid tool name placeholder")
    void shouldReturnMalformedCallErrorForInvalidToolNamePlaceholder() {
        ToolUseBlock invalidCall =
                ToolUseBlock.builder()
                        .id("call-invalid")
                        .name(ToolUseBlock.INVALID_TOOL_NAME)
                        .input(Map.of())
                        .content("{}")
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(invalidCall), null, null, null).block(TIMEOUT);

        assertNotNull(responses, "Executor should return a response for the invalid call");
        assertEquals(1, responses.size(), "Invalid call should yield one response");

        String content = extractFirstText(responses.get(0));
        assertNotNull(content, "Error content should be present");
        assertTrue(
                content.contains("missing function name"),
                "Error should explain the call was malformed, got: " + content);
    }

    @Test
    @DisplayName("Should wrap tool errors inside executor response")
    void shouldReturnErrorWhenToolThrows() {
        Map<String, Object> errorInput = Map.of("message", "test failure");
        ToolUseBlock errorCall =
                ToolUseBlock.builder()
                        .id("call-error")
                        .name("error_tool")
                        .input(errorInput)
                        .content(JsonUtils.getJsonCodec().toJson(errorInput))
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(errorCall), null, null, null).block(TIMEOUT);

        assertNotNull(responses, "Executor should return an error response");
        assertEquals(1, responses.size(), "Single failing call should yield one response");

        String content = extractFirstText(responses.get(0));
        assertEquals(
                "Error: Tool execution failed: Tool error: test failure",
                content,
                "Error message should be wrapped by executor");
    }

    @Test
    @DisplayName("Should convert empty tool publishers to error responses")
    void shouldReturnErrorWhenToolCompletesEmpty() {
        toolkit.registerTool(
                new AgentTool() {
                    @Override
                    public String getName() {
                        return "empty_tool";
                    }

                    @Override
                    public String getDescription() {
                        return "Tool that completes without a result";
                    }

                    @Override
                    public Map<String, Object> getParameters() {
                        return Map.of("type", "object", "properties", Map.of());
                    }

                    @Override
                    public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
                        return Mono.empty();
                    }
                });

        Map<String, Object> emptyInput = Map.of();
        ToolUseBlock emptyCall =
                ToolUseBlock.builder()
                        .id("call-empty")
                        .name("empty_tool")
                        .input(emptyInput)
                        .content(JsonUtils.getJsonCodec().toJson(emptyInput))
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(emptyCall), null, null, null).block(TIMEOUT);

        assertNotNull(responses, "Executor should return an error response");
        assertEquals(1, responses.size(), "Empty completion should still yield one response");
        assertEquals("call-empty", responses.get(0).getId(), "Response should keep tool call id");
        assertEquals("empty_tool", responses.get(0).getName(), "Response should keep tool name");
        assertEquals(
                "Error: Tool execution failed: Tool completed without returning a result",
                extractFirstText(responses.get(0)));
    }

    @Test
    @DisplayName("Should NOT specially handle InterruptedException in error path")
    void testToolErrorWithoutInterruptSpecialCase() {
        // Create a tool that throws RuntimeException with InterruptedException cause
        toolkit.registerTool(
                new AgentTool() {
                    @Override
                    public String getName() {
                        return "interrupted_tool";
                    }

                    @Override
                    public String getDescription() {
                        return "Tool that simulates interrupted error";
                    }

                    @Override
                    public Map<String, Object> getParameters() {
                        Map<String, Object> schema = new HashMap<>();
                        schema.put("type", "object");
                        schema.put("properties", new HashMap<>());
                        return schema;
                    }

                    @Override
                    public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
                        return Mono.error(
                                new RuntimeException(
                                        "Execution error",
                                        new InterruptedException("Thread interrupted")));
                    }
                });

        Map<String, Object> emptyInput = Map.of();
        ToolUseBlock interruptedCall =
                ToolUseBlock.builder()
                        .id("call-interrupt")
                        .name("interrupted_tool")
                        .input(emptyInput)
                        .content(JsonUtils.getJsonCodec().toJson(emptyInput))
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(interruptedCall), null, null, null).block(TIMEOUT);

        assertNotNull(responses, "Should return error response");
        assertEquals(1, responses.size(), "Should have one response");

        String errorText = extractFirstText(responses.get(0));
        // Should be standard error format, not special interrupted result
        assertTrue(
                errorText.startsWith("Error:"), "Should use standard error format: " + errorText);
        assertTrue(
                errorText.contains("Tool execution failed")
                        || errorText.contains("Execution error"),
                "Should contain error message");
    }

    @Test
    @DisplayName("Should handle concurrent tool execution with errors")
    void testConcurrentToolExecutionWithErrors() {
        // Register a tool that sometimes fails (thread-safe counter)
        AtomicInteger callCount = new AtomicInteger(0);
        toolkit.registerTool(
                new AgentTool() {
                    @Override
                    public String getName() {
                        return "flaky_tool";
                    }

                    @Override
                    public String getDescription() {
                        return "Tool that fails on first call";
                    }

                    @Override
                    public Map<String, Object> getParameters() {
                        Map<String, Object> schema = new HashMap<>();
                        schema.put("type", "object");
                        schema.put("properties", new HashMap<>());
                        return schema;
                    }

                    @Override
                    public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
                        int count = callCount.getAndIncrement();
                        if (count == 0) {
                            return Mono.error(new RuntimeException("First call failed"));
                        }
                        return Mono.just(
                                ToolResultBlock.of(
                                        TextBlock.builder()
                                                .text("Call " + count + " succeeded")
                                                .build()));
                    }
                });

        // Execute multiple calls in parallel
        Map<String, Object> emptyInput = Map.of();
        Map<String, Object> addInput = Map.of("a", 1, "b", 2);
        ToolUseBlock call1 =
                ToolUseBlock.builder()
                        .id("call-1")
                        .name("flaky_tool")
                        .input(emptyInput)
                        .content(JsonUtils.getJsonCodec().toJson(emptyInput))
                        .build();
        ToolUseBlock call2 =
                ToolUseBlock.builder()
                        .id("call-2")
                        .name("flaky_tool")
                        .input(emptyInput)
                        .content(JsonUtils.getJsonCodec().toJson(emptyInput))
                        .build();
        ToolUseBlock call3 =
                ToolUseBlock.builder()
                        .id("call-3")
                        .name("add")
                        .input(addInput)
                        .content(JsonUtils.getJsonCodec().toJson(addInput))
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(call1, call2, call3), null, null, null).block(TIMEOUT);

        assertNotNull(responses, "Should return responses");
        assertEquals(3, responses.size(), "Should have three responses");

        // Count how many calls succeeded vs failed
        long errorCount =
                responses.stream().filter(r -> extractFirstText(r).startsWith("Error:")).count();
        long successCount =
                responses.stream()
                        .filter(
                                r ->
                                        extractFirstText(r).contains("succeeded")
                                                || extractFirstText(r).equals("3"))
                        .count();

        // Exactly one flaky_tool call should fail (the first one to execute)
        // and two should succeed (one flaky_tool + one add)
        assertEquals(1, errorCount, "Exactly one call should fail");
        assertEquals(2, successCount, "Exactly two calls should succeed");
    }

    @Test
    @DisplayName("Should apply preset parameters after explicit ToolCallParam input")
    void shouldApplyPresetParametersAfterExplicitInput() {
        class OverrideTool {
            @Tool(description = "Test preset precedence with explicit ToolCallParam input")
            public ToolResultBlock testOverride(
                    @ToolParam(name = "param1") String param1,
                    @ToolParam(name = "param2") String param2) {
                return ToolResultBlock.text(
                        String.format("param1: %s, param2: %s", param1, param2));
            }
        }

        toolkit.registration()
                .tool(new OverrideTool())
                .presetParameters(
                        Map.of(
                                "testOverride",
                                Map.of("param1", "preset_value1", "param2", "preset_value2")))
                .apply();

        Map<String, Object> explicitInput = Map.of("param1", "agent_value1");
        ToolUseBlock toolCall =
                ToolUseBlock.builder()
                        .id("call-override")
                        .name("testOverride")
                        .input(Map.of())
                        .content("{}")
                        .build();

        ToolResultBlock result =
                toolkit.callTool(
                                ToolCallParam.builder()
                                        .toolUseBlock(toolCall)
                                        .input(explicitInput)
                                        .build())
                        .block(TIMEOUT);

        assertNotNull(result, "Result should not be null");
        String resultText = extractFirstText(result);
        assertTrue(
                resultText.contains("param1: preset_value1"),
                "Preset value should override explicit ToolCallParam input");
        assertTrue(resultText.contains("param2: preset_value2"), "Preset value should be used");
    }

    @Test
    @DisplayName("Should use only preset parameters when both input sources are absent")
    void shouldUseOnlyPresetParametersWhenInputsAbsent() {
        class PresetOnlyTool {
            @Tool(description = "Test preset usage when no explicit inputs are present")
            public ToolResultBlock presetOnly(@ToolParam(name = "param1") String param1) {
                return ToolResultBlock.text("param1: " + param1);
            }
        }

        toolkit.registration()
                .tool(new PresetOnlyTool())
                .presetParameters(Map.of("presetOnly", Map.of("param1", "preset_value1")))
                .apply();

        ToolUseBlock toolCall =
                ToolUseBlock.builder()
                        .id("call-preset-only")
                        .name("presetOnly")
                        .content("{}")
                        .build();

        ToolResultBlock result =
                toolkit.callTool(ToolCallParam.builder().toolUseBlock(toolCall).build())
                        .block(TIMEOUT);

        assertNotNull(result, "Result should not be null");
        assertEquals("param1: preset_value1", extractFirstText(result));
    }

    @Test
    @DisplayName("Should execute without preset parameters when registration metadata is absent")
    void shouldExecuteWhenRegisteredMetadataIsAbsent() {
        AgentTool echoTool =
                new AgentTool() {
                    @Override
                    public String getName() {
                        return "metadata_gap_tool";
                    }

                    @Override
                    public String getDescription() {
                        return "Tool for simulating a metadata lookup gap";
                    }

                    @Override
                    public Map<String, Object> getParameters() {
                        return Map.of("type", "object", "properties", Map.of());
                    }

                    @Override
                    public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
                        return Mono.just(
                                ToolResultBlock.text("value: " + param.getInput().get("value")));
                    }
                };

        ToolRegistry registryWithMetadataGap =
                new ToolRegistry() {
                    @Override
                    RegisteredToolFunction getRegisteredTool(String name) {
                        return null;
                    }
                };
        registryWithMetadataGap.registerTool(
                echoTool.getName(), echoTool, new RegisteredToolFunction(echoTool, null, null));
        ToolExecutor executor =
                new ToolExecutor(
                        toolkit,
                        registryWithMetadataGap,
                        new ToolGroupManager(),
                        ToolkitConfig.defaultConfig());
        Map<String, Object> input = Map.of("value", "caller_value");
        ToolUseBlock toolCall =
                ToolUseBlock.builder()
                        .id("call-metadata-gap")
                        .name(echoTool.getName())
                        .input(input)
                        .content(JsonUtils.getJsonCodec().toJson(input))
                        .build();

        ToolResultBlock result =
                executor.execute(ToolCallParam.builder().toolUseBlock(toolCall).build())
                        .block(TIMEOUT);

        assertNotNull(result, "Result should not be null");
        assertEquals("value: caller_value", extractFirstText(result));
    }

    @Test
    @DisplayName("Should format all error messages consistently")
    void testErrorMessageFormat() {
        // Register various failing tools
        toolkit.registerTool(
                new AgentTool() {
                    @Override
                    public String getName() {
                        return "null_pointer_tool";
                    }

                    @Override
                    public String getDescription() {
                        return "Tool that throws NPE";
                    }

                    @Override
                    public Map<String, Object> getParameters() {
                        return Map.of("type", "object", "properties", new HashMap<>());
                    }

                    @Override
                    public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
                        return Mono.error(new NullPointerException("Null value encountered"));
                    }
                });

        toolkit.registerTool(
                new AgentTool() {
                    @Override
                    public String getName() {
                        return "illegal_arg_tool";
                    }

                    @Override
                    public String getDescription() {
                        return "Tool that throws IllegalArgumentException";
                    }

                    @Override
                    public Map<String, Object> getParameters() {
                        return Map.of("type", "object", "properties", new HashMap<>());
                    }

                    @Override
                    public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
                        return Mono.error(
                                new IllegalArgumentException("Invalid argument provided"));
                    }
                });

        // Execute both tools
        Map<String, Object> emptyInput = Map.of();
        ToolUseBlock npeCall =
                ToolUseBlock.builder()
                        .id("npe")
                        .name("null_pointer_tool")
                        .input(emptyInput)
                        .content(JsonUtils.getJsonCodec().toJson(emptyInput))
                        .build();
        ToolUseBlock argCall =
                ToolUseBlock.builder()
                        .id("arg")
                        .name("illegal_arg_tool")
                        .input(emptyInput)
                        .content(JsonUtils.getJsonCodec().toJson(emptyInput))
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(npeCall, argCall), null, null, null).block(TIMEOUT);

        assertNotNull(responses, "Should return responses");
        assertEquals(2, responses.size(), "Should have two responses");

        // Both should follow same error format
        for (int i = 0; i < responses.size(); i++) {
            String errorText = extractFirstText(responses.get(i));
            assertTrue(
                    errorText.startsWith("Error:"),
                    "Error " + i + " should start with 'Error:': " + errorText);
            assertTrue(
                    errorText.contains("Tool execution failed")
                            || errorText.contains("encountered")
                            || errorText.contains("provided"),
                    "Error " + i + " should contain meaningful message: " + errorText);
        }
    }

    @Test
    @DisplayName("Should skip remaining sequential tools once agent interrupt is pending")
    void shouldSkipRemainingToolsAfterInterrupt() {
        toolkit.registerTool(
                new AgentTool() {
                    @Override
                    public String getName() {
                        return "interrupting_tool";
                    }

                    @Override
                    public String getDescription() {
                        return "Tool that triggers an agent interrupt while running";
                    }

                    @Override
                    public Map<String, Object> getParameters() {
                        return Map.of("type", "object", "properties", Map.of());
                    }

                    @Override
                    public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
                        return Mono.fromCallable(
                                () -> {
                                    param.getAgent().interrupt();
                                    return ToolResultBlock.text("first done");
                                });
                    }
                });

        InterruptibleTestAgent agent = new InterruptibleTestAgent();
        Map<String, Object> emptyInput = Map.of();
        Map<String, Object> addInput = Map.of("a", 1, "b", 2);
        ToolUseBlock first =
                ToolUseBlock.builder()
                        .id("call-first")
                        .name("interrupting_tool")
                        .input(emptyInput)
                        .content(JsonUtils.getJsonCodec().toJson(emptyInput))
                        .build();
        ToolUseBlock second =
                ToolUseBlock.builder()
                        .id("call-second")
                        .name("add")
                        .input(addInput)
                        .content(JsonUtils.getJsonCodec().toJson(addInput))
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(first, second), null, agent, null).block(TIMEOUT);

        assertNotNull(responses, "Executor should return responses");
        assertEquals(2, responses.size(), "Skipped tool must still yield a result");
        assertEquals("first done", extractFirstText(responses.get(0)));
        assertEquals("call-second", responses.get(1).getId(), "Skipped result should keep id");
        assertEquals("add", responses.get(1).getName(), "Skipped result should keep name");
        assertTrue(
                extractFirstText(responses.get(1)).contains("Tool execution skipped"),
                "Second tool should be skipped due to pending interrupt");
    }

    @Test
    @DisplayName("Should execute all tools when agent has no pending interrupt")
    void shouldExecuteAllToolsWithoutPendingInterrupt() {
        InterruptibleTestAgent agent = new InterruptibleTestAgent();
        Map<String, Object> addInput = Map.of("a", 3, "b", 4);
        ToolUseBlock addCall =
                ToolUseBlock.builder()
                        .id("call-add-guarded")
                        .name("add")
                        .input(addInput)
                        .content(JsonUtils.getJsonCodec().toJson(addInput))
                        .build();

        List<ToolResultBlock> responses =
                toolkit.callTools(List.of(addCall), null, agent, null).block(TIMEOUT);

        assertNotNull(responses, "Executor should return responses");
        assertEquals(1, responses.size(), "Tool call should be executed");
        assertEquals("7", extractFirstText(responses.get(0)), "Guard must not alter execution");
    }

    private String extractFirstText(ToolResultBlock response) {
        assertTrue(
                ToolTestUtils.isValidToolResultBlock(response),
                "Tool response should contain content");
        List<ContentBlock> outputs = response.getOutput();
        if (outputs.isEmpty()) return "";
        return ((TextBlock) outputs.get(0)).getText();
    }

    /** Minimal Agent implementation backed by a real interrupt flag for guard tests. */
    private static final class InterruptibleTestAgent implements Agent {
        private final AtomicBoolean interrupted = new AtomicBoolean(false);

        @Override
        public String getAgentId() {
            return "interruptible-test-agent";
        }

        @Override
        public String getName() {
            return "InterruptibleTestAgent";
        }

        @Override
        public void interrupt() {
            interrupted.set(true);
        }

        @Override
        public void interrupt(Msg msg) {
            interrupted.set(true);
        }

        @Override
        public boolean isInterrupted() {
            return interrupted.get();
        }

        @Override
        public Mono<Msg> call(Msg msg) {
            return Mono.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Mono<Msg> call(List<Msg> msgs) {
            return Mono.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Mono<Msg> call(List<Msg> msgs, Class<?> structuredOutputClass) {
            return Mono.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Mono<Msg> call(List<Msg> msgs, JsonNode schema) {
            return Mono.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Flux<Event> stream(Msg msg, StreamOptions options) {
            return Flux.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Flux<Event> stream(List<Msg> msgs, StreamOptions options) {
            return Flux.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Flux<Event> stream(Msg msg, StreamOptions options, Class<?> structuredModel) {
            return Flux.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Flux<Event> stream(List<Msg> msgs, StreamOptions options, Class<?> structuredModel) {
            return Flux.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Flux<Event> stream(List<Msg> msgs, StreamOptions options, JsonNode schema) {
            return Flux.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Mono<Void> observe(Msg msg) {
            return Mono.error(new UnsupportedOperationException("not used"));
        }

        @Override
        public Mono<Void> observe(List<Msg> msgs) {
            return Mono.error(new UnsupportedOperationException("not used"));
        }
    }
}
