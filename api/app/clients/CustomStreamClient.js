const { HttpsProxyAgent } = require('https-proxy-agent');
const { sendEvent } = require('@librechat/api');
const { ContentTypes } = require('librechat-data-provider');
const { createFetch, resolveHeaders } = require('@librechat/api');
const BaseClient = require('./BaseClient');
const { logger } = require('~/config');
const { pipeline } = require('stream/promises');
const { sleep } = require('~/server/utils');

/**
 * Custom streaming client for endpoints that return OpenAI-format tool results
 * but need to bypass the LangChain agent system for direct streaming
 */
class CustomStreamClient extends BaseClient {
  constructor(apiKey, options = {}) {
    super(apiKey, options);
    
    // Set options for BaseClient's fetch method to work properly
    this.options = {
      ...options,
      // Override BaseClient's directEndpoint behavior
      directEndpoint: false,
      reverseProxyUrl: undefined
    };
    
    this.apiKey = apiKey;
    this.baseURL = options.baseURL;
    this.model = options.model || 'default';
    this.headers = options.headers || {};
    this.proxy = options.proxy;
    
    // Set streaming rate for real-time effect
    this.streamRate = options.streamRate || 20; // Default 20ms like other clients
    
    // Set tool completion buffer to prevent rapid-fire completions (like standard agents)
    this.streamBuffer = options.streamBuffer || 1000; // Default 1 second buffer between tool completions
    this.lastToolCompletionTime = null; // Track when last tool completion was sent
    
    // Debug log to check what baseURL we received
    logger.info(`[CustomStreamClient] DEBUG: Constructor: baseURL="${this.baseURL}", model="${this.model}", hasApiKey=${!!this.apiKey}`);
    
    /** @type {Map<string, Object>} */
    this.toolCallMap = new Map();
    /** @type {number} */
    this.stepIndex = 0;
    /** @type {number} */
    this.contentIndex = 0; // Track position in message content array for proper ordering
    /** @type {Promise} */
    this.lastContentUpdate = Promise.resolve(); // Serialize content updates to prevent race conditions
    /** @type {Array} */
    this.contentParts = [];
    /** @type {boolean} */
    this.messageCreationSent = false; // Track if we've sent the initial message creation step
    /** @type {string|null} */
    this.messageCreationStepId = null; // Store message creation step ID for tool_call_ids association
    /** @type {Set<string>} */
    this.pendingToolCalls = new Set(); // Track tool calls that haven't completed yet
    /** @type {boolean} */
    this.streamEnded = false; // Track if the main stream has ended
    /** @type {Function|null} */
    this.streamCompletionResolver = null; // Promise resolver for waiting on tool completion

  }

  setOptions(options) {
    this.options = { ...this.options, ...options };
    return this;
  }

  getSaveOptions() {
    return {
      endpoint: this.options.endpoint,
      model: this.model,
      baseURL: this.baseURL,
    };
  }

  getBuildMessagesOptions() {
    return {};
  }

  checkVisionRequest() {
    // No vision handling needed for this client
  }

  /**
   * Converts OpenAI format messages to our endpoint format
   * @param {Array} messages - LangChain formatted messages
   * @returns {Array} - OpenAI compatible messages
   */
  formatMessages(messages) {
    return messages.map(msg => {
      if (msg.role || msg._getType) {
        // Already in OpenAI format or LangChain message
        const role = msg.role || (msg._getType && msg._getType() === 'human' ? 'user' : 'assistant');
        return {
          role,
          content: typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content)
        };
      }
      return msg;
    });
  }

  /**
   * Handles tool call conversion from OpenAI format to agent events
   * @param {Object} chunk - The streaming chunk
   */
  async handleToolCallConversion(chunk) {
    if (!chunk.choices || !Array.isArray(chunk.choices)) {
      return;
    }

    for (const choice of chunk.choices) {
      const delta = choice.delta;
      if (!delta) {
        return;
      }

      // Handle tool call initiation (when tool_calls array is present)
      if (delta.tool_calls && Array.isArray(delta.tool_calls)) {
        const toolCallsTimestamp = new Date().toISOString();
        const toolCallsStartTime = Date.now();
        logger.info(`[CustomStreamClient] [${toolCallsTimestamp}] DEBUG: Processing ${delta.tool_calls.length} tool calls in chunk (startTime: ${toolCallsStartTime})`);
        
        // CRITICAL FIX: Send MESSAGE_CREATION step first (like standard agents)
        if (!this.messageCreationSent) {
          const messageCreationStepId = `message_step_${Date.now()}`;
          const runId = this.responseMessageId || `run_${Date.now()}`;
          
          logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Sending MESSAGE_CREATION step first`);
          
          if (this.options.res) {
            sendEvent(this.options.res, {
              event: 'on_run_step',
              data: {
                id: messageCreationStepId,
                runId: runId,
                index: 0,
                type: 'message_creation',
                stepDetails: {
                  type: 'message_creation',
                  message_creation: {
                    message_id: runId,
                  }
                }
              }
            });
            
            if (this.options.res.flush) {
              this.options.res.flush();
            }
          }
          
          this.messageCreationSent = true;
          this.messageCreationStepId = messageCreationStepId; // Store for tool_call_ids association
        }
        
        // Send message delta with tool_call_ids (like standard agents)
        const allToolCallIds = delta.tool_calls.map(tc => tc.id);
        if (this.messageCreationStepId && this.options.res) {
          logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Sending message delta with tool_call_ids: ${allToolCallIds.join(', ')}`);
          
          sendEvent(this.options.res, {
            event: 'on_message_delta',
            data: {
              id: this.messageCreationStepId,
              delta: {
                content: [
                  {
                    type: 'text',
                    text: '',
                    tool_call_ids: allToolCallIds,
                  },
                ],
              },
            },
          });
          
          if (this.options.res.flush) {
            this.options.res.flush();
          }
        }
        
        delta.tool_calls.forEach((toolCall, index) => {
          if (toolCall.function && toolCall.id) {
            const toolCallId = toolCall.id;
            const toolName = toolCall.function.name || 'unknown_tool';
            const individualStartTime = Date.now();
            let args = {};
            
            // Parse arguments if they exist
            try {
              if (toolCall.function.arguments) {
                args = JSON.parse(toolCall.function.arguments);
              }
            } catch (e) {
              logger.debug(`[CustomStreamClient] Could not parse tool arguments: ${toolCall.function.arguments}`);
              args = { raw_arguments: toolCall.function.arguments };
            }

            // Generate unique step ID to prevent conflicts between tool calls
            const stepId = `step_${Date.now()}_${this.stepIndex++}_${toolCallId.slice(-8)}`;
            const runId = this.responseMessageId || `run_${Date.now()}`;

            // Serialize tool call processing to prevent race conditions
            this.lastContentUpdate = this.lastContentUpdate.then(async () => {
              // Store tool call info with actual name and args, including content index
              const toolCallContentIndex = this.contentIndex; // Store current index before incrementing
              this.toolCallMap.set(toolCallId, {
                stepId,
                runId,
                toolName,
                args,
                startTime: individualStartTime,
                initiationTimestamp: new Date().toISOString(),
                contentIndex: toolCallContentIndex, // Store for completion event
              });

            // Track this tool call as pending
            this.pendingToolCalls.add(toolCallId);
            logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Added tool call to pending: ${toolCallId} (total pending: ${this.pendingToolCalls.size})`);

            // Create tool call content part with proper type for frontend
            const toolCallContent = {
              type: ContentTypes.TOOL_CALL,
              tool_call: {
                id: toolCallId,
                name: toolName,
                args: JSON.stringify(args),
                type: 'tool_call', // Set ToolCallTypes.TOOL_CALL for frontend
                progress: 0.1, // Initial progress for immediate display
              },
            };

            this.contentParts.push(toolCallContent);

            // Send on_run_step event (creates the tool call UI)
            if (this.options.res) {
              const eventSendTime = Date.now();
              const eventTimestamp = new Date().toISOString();
              const timeSinceStart = eventSendTime - individualStartTime;
              
              logger.info(`[CustomStreamClient] [${eventTimestamp}] DEBUG: Sending tool call ${index + 1}/${delta.tool_calls.length}: ${toolName} with args: ${JSON.stringify(args)} (processed in ${timeSinceStart}ms)`);
              
              // Send the exact event structure that the frontend expects
              sendEvent(this.options.res, {
                event: 'on_run_step',
                data: {
                  id: stepId,
                  runId: runId,
                  index: toolCallContentIndex, // Use stored content index for proper positioning
                  type: 'tool_calls',
                  stepDetails: {
                    type: 'tool_calls',
                    tool_calls: [{
                      id: toolCallId,
                      name: toolName,
                      args: JSON.stringify(args),
                      type: 'tool_call'
                    }]
                  }
                }
              });
              
              const afterEventTime = Date.now();
              logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Tool call event sent in ${afterEventTime - eventSendTime}ms, flushing connection`);
              
              // Ensure tool call events are immediately sent
              if (this.options.res.flush) {
                this.options.res.flush();
              }
              
              // Increment content index since tool call takes up a position in content array
              this.contentIndex++;
              logger.debug(`[CustomStreamClient] [${new Date().toISOString()}] Incremented content index to ${this.contentIndex} after tool call`);
              
              // POTENTIAL FIX: Force a micro-task delay to ensure React processes the update
              setTimeout(() => {
                logger.info(`[CustomStreamClient] DEBUG: Tool call UI should be visible now for ${toolName}`);
              }, 10);
            }
            }); // Close the serialization promise
          }
        });
      }

      // Handle tool results (role: "tool")
      if (delta.role === 'tool' && delta.tool_call_id && delta.content) {
        const completionStartTime = Date.now();
        const completionTimestamp = new Date().toISOString();
        const toolCallId = delta.tool_call_id;
        const content = delta.content;

        // Get tool call info (should exist from the initial tool call)
        const toolInfo = this.toolCallMap.get(toolCallId);
        if (!toolInfo) {
          logger.warn(`[CustomStreamClient] [${completionTimestamp}] Tool call result received for unknown ID: ${toolCallId}`);
          return;
        }

        const totalToolTime = completionStartTime - toolInfo.startTime;
        logger.info(`[CustomStreamClient] [${completionTimestamp}] DEBUG: Processing tool completion for ${toolInfo.toolName} (total time: ${totalToolTime}ms since initiation)`);

        // Update the content part with the result
        const toolCallIndex = this.contentParts.findIndex(
          part => part.type === ContentTypes.TOOL_CALL && 
                 part.tool_call.id === toolCallId
        );
        
        if (toolCallIndex !== -1) {
          this.contentParts[toolCallIndex].tool_call.output = content;
          this.contentParts[toolCallIndex].tool_call.progress = 1;
        }

        // Send completion event with proper timing buffer (like standard agents)
        await this.sendToolCompletionEvent(toolInfo, toolCallId, content, toolInfo.contentIndex);
      }
    }
  }

  /**
   * Main streaming method that connects to your custom endpoint
   * @param {Object} payload - The request payload
   * @param {Object} opts - Options including onProgress callback
   */
  async sendCompletion(payload, opts = {}) {
    try {
      // Validate baseURL
      if (!this.baseURL) {
        throw new Error(`[CustomStreamClient] baseURL is required but was: ${this.baseURL}`);
      }
      
      logger.info(`[CustomStreamClient] DEBUG: sendCompletion starting with baseURL="${this.baseURL}"`);
      
      const streamingStartTime = Date.now();
      const streamingStartTimestamp = new Date().toISOString();
      logger.info(`[CustomStreamClient] [${streamingStartTimestamp}] DEBUG: Starting streaming request (startTime: ${streamingStartTime})`);
      
      const messages = this.formatMessages(payload);
      
      const requestBody = {
        model: this.model,
        messages: messages,
        stream: true
      };

      logger.info(`[CustomStreamClient] DEBUG: Request body prepared with ${messages.length} messages`);

      const fetchOptions = {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.apiKey}`,
          ...resolveHeaders(this.headers),
        },
        body: JSON.stringify(requestBody),
      };

      if (this.proxy) {
        fetchOptions.agent = new HttpsProxyAgent(this.proxy);
      }

      // Construct the full URL - ensure we don't double-add /chat/completions if baseURL already includes it
      let fullURL;
      if (this.baseURL.includes('/chat/completions')) {
        fullURL = this.baseURL;
      } else if (this.baseURL.endsWith('/v1')) {
        fullURL = `${this.baseURL}/chat/completions`;
      } else {
        fullURL = `${this.baseURL}/v1/chat/completions`;
      }
      
      const fetchStartTime = Date.now();
      const fetchStartTimestamp = new Date().toISOString();
      logger.info(`[CustomStreamClient] [${fetchStartTimestamp}] DEBUG: Making request to: ${fullURL} (fetchStart: ${fetchStartTime})`);
      
      // Use native fetch directly for proper streaming support
      const response = await fetch(fullURL, fetchOptions);
      
      const responseReceivedTime = Date.now();
      const fetchDuration = responseReceivedTime - fetchStartTime;
      logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Response received in ${fetchDuration}ms`);

      if (!response.ok) {
        const errorText = await response.text().catch(() => 'Unable to read error response');
        logger.error(`[CustomStreamClient] Request failed: ${response.status} ${response.statusText} - ${errorText}`);
        throw new Error(`HTTP ${response.status}: ${response.statusText} - ${errorText}`);
      }

      logger.info(`[CustomStreamClient] DEBUG: Response received, body type: ${typeof response.body}, has getReader: ${!!response.body?.getReader}`);

      let text = '';

      // Try different streaming approaches based on what's available
      if (response.body && typeof response.body.getReader === 'function') {
        // Browser-like fetch with ReadableStream
        const reader = response.body.getReader();
        const decoder = new TextDecoder();

        try {
          let chunkIndex = 0;
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            const chunk = decoder.decode(value, { stream: true });
            const chunkProcessStart = Date.now();
            const chunkTimestamp = new Date().toISOString();
            
            logger.debug(`[CustomStreamClient] [${chunkTimestamp}] Processing chunk ${chunkIndex++}: "${chunk.substring(0, 100)}..." (raw size: ${chunk.length} bytes)`);
            
            const chunkText = await this.processStreamChunk(chunk, opts);
            text += chunkText;
            
            const chunkProcessEnd = Date.now();
            const chunkDuration = chunkProcessEnd - chunkProcessStart;
            logger.debug(`[CustomStreamClient] [${new Date().toISOString()}] Chunk ${chunkIndex - 1} processed in ${chunkDuration}ms, extracted text: "${chunkText.substring(0, 50)}..." (extracted length: ${chunkText.length})`);
          }
        } finally {
          reader.releaseLock();
        }
      } else if (response.body && typeof response.body[Symbol.asyncIterator] === 'function') {
        // Node.js style async iterable
        const decoder = new TextDecoder();
        let chunkIndex = 0;
        for await (const chunk of response.body) {
          const chunkStr = decoder.decode(chunk, { stream: true });
          logger.debug(`[CustomStreamClient] Processing async chunk ${chunkIndex++}: "${chunkStr.substring(0, 100)}..."`);
          
          const chunkText = await this.processStreamChunk(chunkStr, opts);
          text += chunkText;
          
          logger.debug(`[CustomStreamClient] Async chunk ${chunkIndex - 1} processed, extracted text: "${chunkText.substring(0, 50)}..."`);
        }
      } else {
        // Fallback: get full response as text (non-streaming)
        logger.warn('[CustomStreamClient] No streaming support detected, falling back to full response');
        const fullText = await response.text();
        text = fullText;
        
        if (opts.onProgress) {
          opts.onProgress(fullText);
        }
      }

      const streamingEndTime = Date.now();
      const totalStreamingDuration = streamingEndTime - streamingStartTime;
      logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Streaming completed in ${totalStreamingDuration}ms, total text length: ${text.length}`);

      // Mark stream as ended, but wait for tool calls to complete
      this.streamEnded = true;
      logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Main stream ended, waiting for ${this.pendingToolCalls.size} pending tool calls`);

      // If there are pending tool calls, wait for them to complete
      if (this.pendingToolCalls.size > 0) {
        await new Promise((resolve) => {
          this.streamCompletionResolver = resolve;
          // Set a timeout to prevent hanging forever (30 seconds max)
          setTimeout(() => {
            if (this.streamCompletionResolver) {
              logger.warn(`[CustomStreamClient] [${new Date().toISOString()}] WARNING: Tool completion timeout, proceeding with ${this.pendingToolCalls.size} pending tool calls`);
              this.streamCompletionResolver();
              this.streamCompletionResolver = null;
            }
          }, 30000);
        });
      }

      logger.info(`[CustomStreamClient] sendCompletion: Stream completed successfully with ${text.length} characters and all tool calls finished`);
      return text;

    } catch (error) {
      logger.error('[CustomStreamClient] Error in sendCompletion:', error);
      throw error;
    }
  }

  /**
   * Send tool completion event (extracted to avoid duplication)
   * @param {Object} toolInfo - Tool call information
   * @param {string} toolCallId - Tool call ID
   * @param {string} content - Tool output content
   * @param {number} index - Step index
   */
  async sendToolCompletionEvent(toolInfo, toolCallId, content, index) {
    if (!this.options.res) return;

    // Apply stream buffer timing (like standard agents) to prevent rapid completions
    if (this.lastToolCompletionTime && this.streamBuffer) {
      const timeSinceLastCompletion = Date.now() - this.lastToolCompletionTime;
      if (timeSinceLastCompletion < this.streamBuffer) {
        const delayNeeded = this.streamBuffer - timeSinceLastCompletion;
        logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Tool completion buffer: waiting ${delayNeeded}ms before sending completion for ${toolInfo.toolName}`);
        await sleep(delayNeeded);
      }
    }

    const eventSendStartTime = Date.now();
    const eventSendTimestamp = new Date().toISOString();
    
    logger.info(`[CustomStreamClient] [${eventSendTimestamp}] DEBUG: Sending tool call completion for ${toolInfo.toolName}`);
    
    sendEvent(this.options.res, {
      event: 'on_run_step_completed',
      data: {
        result: {
          id: toolInfo.stepId,
          index: index,
          type: 'tool_call',
          tool_call: {
            id: toolCallId,
            name: toolInfo.toolName,
            args: JSON.stringify(toolInfo.args),
            output: content,
            progress: 1
          }
        }
      }
    });
    
    // Update last completion time for buffer calculation
    this.lastToolCompletionTime = Date.now();
    
    // Remove from pending tool calls
    this.pendingToolCalls.delete(toolCallId);
    logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Removed tool call from pending: ${toolCallId} (remaining: ${this.pendingToolCalls.size})`);
    
    const eventSentTime = Date.now();
    const eventDuration = eventSentTime - eventSendStartTime;
    logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Tool completion event sent in ${eventDuration}ms, flushing connection`);
    
    // Ensure tool completion events are immediately sent
    if (this.options.res.flush) {
      this.options.res.flush();
    }

    // Check if all tool calls are complete and main stream ended
    this.checkAndFinishStream();
  }

  /**
   * Check if stream can be finished (all tool calls complete)
   */
  checkAndFinishStream() {
    if (this.streamEnded && this.pendingToolCalls.size === 0) {
      logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: All tool calls complete, resolving stream completion`);
      if (this.streamCompletionResolver) {
        this.streamCompletionResolver();
        this.streamCompletionResolver = null;
      }
    } else {
      logger.info(`[CustomStreamClient] [${new Date().toISOString()}] DEBUG: Stream completion waiting - streamEnded: ${this.streamEnded}, pendingToolCalls: ${this.pendingToolCalls.size}`);
    }
  }

  /**
   * Process a streaming chunk and extract tool calls/content
   * @param {string} chunk - The raw chunk string
   * @param {Object} opts - Options with onProgress callback
   * @returns {Promise<string>} - Any text content found
   */
  async processStreamChunk(chunk, opts) {
    const startTime = Date.now();
    let textContent = '';
    const lines = chunk.split('\n');

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const data = line.slice(6).trim();
        
        if (data === '[DONE]') {
          continue;
        }

        try {
          const parseStart = Date.now();
          const parsed = JSON.parse(data);
          const parseEnd = Date.now();
          
          if (parseEnd - parseStart > 5) { // Only log if parsing takes more than 5ms
            logger.debug(`[CustomStreamClient] JSON parse took ${parseEnd - parseStart}ms for data: "${data.substring(0, 200)}..."`);
          }
          
          // STEP 1: Handle content streaming FIRST for real-time effect
          // Process all choices for content (usually just one, but handle multiple)
          if (parsed.choices && Array.isArray(parsed.choices)) {
            for (const choice of parsed.choices) {
              if (!choice?.delta) continue;
              
              const delta = choice.delta;
              
              // Skip tool content entirely - it's handled by handleToolCallConversion
              if (delta.role === 'tool') {
                logger.debug(`[CustomStreamClient] Skipping tool content from text stream: tool_call_id=${delta.tool_call_id}`);
                continue; // Skip this choice
              }
              
              // Only process content from assistant messages (or no role specified)
              if (delta.content && (!delta.role || delta.role === 'assistant')) {
                const contentProcessingStart = Date.now();
                const contentTimestamp = new Date().toISOString();
                const content = delta.content;
                let processedContent;
                
                processedContent = `\n🤖 ${content}\n`;
                logger.info(`[CustomStreamClient] [${contentTimestamp}] Wrapped intermediate chunk`);
                
                textContent += processedContent;
                
                // Ensure we have a message creation step for the text content
                if (!this.messageCreationSent && this.options.res) {
                  const messageCreationStepId = `message_step_${Date.now()}`;
                  const runId = this.responseMessageId || `run_${Date.now()}`;
                  
                  logger.info(`[CustomStreamClient] [${new Date().toISOString()}] Creating MESSAGE_CREATION step for text streaming`);
                  
                  sendEvent(this.options.res, {
                    event: 'on_run_step',
                    data: {
                      id: messageCreationStepId,
                      runId: runId,
                      index: 0,
                      type: 'message_creation',
                      stepDetails: {
                        type: 'message_creation',
                        message_creation: {
                          message_id: runId,
                        }
                      }
                    }
                  });
                  
                  if (this.options.res.flush) {
                    this.options.res.flush();
                  }
                  
                  this.messageCreationSent = true;
                  this.messageCreationStepId = messageCreationStepId;
                }

                // Send each text chunk immediately via on_message_delta (like standard agents)
                if (this.options.res && this.messageCreationStepId) {
                  // Serialize content updates to prevent race conditions
                  this.lastContentUpdate = this.lastContentUpdate.then(async () => {
                    // Add spacing between chunks for better visual separation
                    // Only add spacing if this isn't the first chunk and it doesn't already have spacing
                    let spacedContent = processedContent;
                    if (this.contentIndex > 0 && !processedContent.startsWith('\n') && !processedContent.startsWith(' ')) {
                      spacedContent = ' ' + processedContent; // Add space before content for separation
                    }
                    
                    logger.debug(`[CustomStreamClient] [${new Date().toISOString()}] Sending immediate text chunk at index ${this.contentIndex}: "${spacedContent.substring(0, 30)}..."`);
                    
                    sendEvent(this.options.res, {
                      event: 'on_message_delta', 
                      data: {
                        id: this.messageCreationStepId,
                        delta: {
                          content: [
                            {
                              type: ContentTypes.TEXT,
                              text: spacedContent, // Send spaced chunk for better visual separation
                            },
                          ],
                        },
                      },
                    });
                    
                    // ALSO add text content to contentParts for final message structure
                    // Check if we need to add a new text part or append to existing one
                    const currentIndex = this.contentIndex;
                    let textPart = this.contentParts[currentIndex];
                    
                    if (!textPart || textPart.type !== ContentTypes.TEXT) {
                      // Create new text content part
                      textPart = {
                        type: ContentTypes.TEXT,
                        text: spacedContent,
                      };
                      this.contentParts[currentIndex] = textPart;
                      logger.debug(`[CustomStreamClient] Created new text part at index ${currentIndex}: "${spacedContent.substring(0, 30)}..."`);
                    } else {
                      // Append to existing text part
                      textPart.text += spacedContent;
                      logger.debug(`[CustomStreamClient] Appended to existing text part at index ${currentIndex}: "${spacedContent.substring(0, 30)}..."`);
                    }
                    
                    // Increment content index for proper tool call positioning
                    this.contentIndex++;
                    logger.debug(`[CustomStreamClient] Content index incremented to ${this.contentIndex} after text chunk`);
                    
                    // Ensure immediate delivery for real-time effect
                    if (this.options.res.flush) {
                      this.options.res.flush();
                    }
                    
                    // Small delay to prevent race conditions with rapid updates
                    await sleep(5);
                  });
                }
                
                const contentProcessingEnd = Date.now();
                logger.debug(`[CustomStreamClient] [${new Date().toISOString()}] Content processing completed in ${contentProcessingEnd - contentProcessingStart}ms`);
              }
              else if (delta.role && delta.role !== 'assistant') {
                logger.debug(`[CustomStreamClient] Skipping content with role: ${delta.role}`);
              }
            }
          }

          // STEP 2: Handle tool call conversion AFTER content is streamed
          const toolCallStart = Date.now();
          await this.handleToolCallConversion(parsed);
          const toolCallEnd = Date.now();
          
          if (toolCallEnd - toolCallStart > 5) { // Only log if tool call processing takes more than 5ms
            logger.debug(`[CustomStreamClient] Tool call conversion took ${toolCallEnd - toolCallStart}ms`);
          }

          // Check for completion
          const hasFinishReason = parsed.choices?.some(choice => choice?.finish_reason);
          if (hasFinishReason) {
            logger.debug(`[CustomStreamClient] Stream completion detected`);
            break;
          }

        } catch (parseError) {
          logger.debug('[CustomStreamClient] Could not parse chunk:', data);
        }
      }
    }

    // Only add delay if we actually processed content to avoid unnecessary waits
    if (textContent.length > 0) {
      logger.debug(`[CustomStreamClient] Adding ${this.streamRate}ms delay after processing ${textContent.length} chars of content`);
      await sleep(this.streamRate);
    }

    const endTime = Date.now();
    logger.debug(`[CustomStreamClient] Chunk processed in ${endTime - startTime}ms, sleep: ${textContent.length > 0 ? this.streamRate : 0}ms, text length: ${textContent.length}`);

    return textContent;
  }

  /**
   * Get the content parts (for agent system compatibility)
   * @returns {Array} Content parts including tool calls
   */
  getContentParts() {
    return this.contentParts;
  }



  /**
   * Generate conversation title using the custom endpoint
   * @param {Object} params - Title generation parameters
   * @param {string} params.text - User message text
   * @param {string} [params.conversationId] - Conversation ID
   * @param {string} [params.responseText] - Assistant response text
   * @param {AbortController} [params.abortController] - Abort controller
   * @returns {Promise<string>} Generated title
   */
  async titleConvo({ text, conversationId, responseText, abortController }) {
    try {
      if (!this.baseURL || !this.apiKey) {
        logger.warn('[CustomStreamClient] titleConvo: Missing baseURL or apiKey');
        return 'New Chat';
      }

      const model = this.modelOptions?.model || this.model || 'gpt-4';
      const truncateText = (str, maxLength = 300) => {
        if (!str) return '';
        return str.length > maxLength ? str.substring(0, maxLength) + '...' : str;
      };

      logger.info(`[CustomStreamClient] titleConvo: Generating title with model ${model}`);

      // Construct the URL for title generation
      let fullURL;
      if (this.baseURL.endsWith('/v1') || this.baseURL.endsWith('/v1/')) {
        fullURL = `${this.baseURL}/chat/completions`;
      } else {
        fullURL = `${this.baseURL}/v1/chat/completions`;
      }

      const instructionsPayload = {
        model: model,
        messages: [
          {
            role: 'user',
            content: `Generate a concise, 5-word-or-less title for this conversation. Use no punctuation and apply title case.
Conversation:
User: "${truncateText(text)}"
Assistant: "${truncateText(responseText)}"
Title:`
          }
        ],
        max_tokens: 20,
        temperature: 0.1,
        stream: false,
      };

      const fetchOptions = {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.apiKey}`,
          ...this.headers,
        },
        body: JSON.stringify(instructionsPayload),
        signal: abortController?.signal,
      };

      logger.info(`[CustomStreamClient] titleConvo: Making request to ${fullURL}`);
      const response = await fetch(fullURL, fetchOptions);

      if (!response.ok) {
        const errorText = await response.text().catch(() => 'Unable to read error response');
        logger.error(`[CustomStreamClient] titleConvo: Request failed: ${response.status} ${response.statusText} - ${errorText}`);
        return 'New Chat';
      }

      const data = await response.json();
      let title = 'New Chat';

      if (data.choices?.[0]?.message?.content) {
        const generatedTitle = data.choices[0].message.content.trim();
        logger.info(`[CustomStreamClient] titleConvo: Raw generated title: "${generatedTitle}"`);

        // Clean up the generated title
        let cleanTitle = generatedTitle.replaceAll('"', '').replaceAll("'", '').replaceAll('`', '').trim();
        
        // If title is too long, truncate to first 5 words
        if (cleanTitle.length > 50) {
          cleanTitle = cleanTitle.split(' ').slice(0, 5).join(' ');
        }
        
        // Remove common prefixes
        cleanTitle = cleanTitle.replace(/^(Title:|TITLE:|Title\s*-\s*)/i, '').replace(/^(A\s+title\s+for\s+this\s+conversation:?)/i, '').trim();
        
        title = cleanTitle || 'New Chat';
        logger.info(`[CustomStreamClient] titleConvo: Final cleaned title: "${title}"`);
      }

      // Record token usage for title generation
      if (data.usage) {
        this.recordTokenUsage({
          promptTokens: data.usage.prompt_tokens || 0,
          completionTokens: data.usage.completion_tokens || 0,
        });
      }

      return title;
    } catch (error) {
      logger.error('[CustomStreamClient] titleConvo: Error generating title:', error);
      return 'New Chat';
    }
  }

  /**
   * Required method for BaseClient compatibility
   */
  async buildMessages(messages, parentMessageId, opts) {
    return {
      prompt: messages,
      messages: messages,
    };
  }

  /**
   * Required method for BaseClient compatibility
   */
  async addImageURLs(message, attachments) {
    return [];
  }

  /**
   * Get token count (simplified for this client)
   */
  getTokenCount(text) {
    return Math.ceil(text.length / 4); // Rough estimate
  }
}

module.exports = CustomStreamClient;