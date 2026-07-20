// Package stream implements SSE token streaming from Ollama to HTTP clients.
//
// Memory: bufio.Scanner with a pooled 64KB buffer avoids per-token allocations.
// Format: OpenAI /v1/completions SSE wire format (compatible with openai-python, LangChain).
// Cancellation: context.Done() is checked on every token — client disconnect aborts immediately.
package stream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const sseMaxLine = 64 * 1024

var bufPool = sync.Pool{New: func() interface{} { b := make([]byte, sseMaxLine); return &b }}

type ollamaChunk struct {
	Response string `json:"response"`
	Done     bool   `json:"done"`
}

type openAIChunk struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []choice `json:"choices"`
}

type choice struct {
	Text         string  `json:"text"`
	Index        int     `json:"index"`
	FinishReason *string `json:"finish_reason"`
}

// Streamer proxies a streaming Ollama response to an HTTP client as SSE.
type Streamer struct {
	upstreamURL string
	httpClient  *http.Client
}

// NewStreamer creates a Streamer that contacts the given Ollama base URL.
func NewStreamer(ollamaBaseURL string) *Streamer {
	return &Streamer{
		upstreamURL: strings.TrimRight(ollamaBaseURL, "/"),
		httpClient:  &http.Client{Timeout: 5 * time.Minute},
	}
}

// Stream sends a streaming generate request and pipes tokens to w as SSE.
func (s *Streamer) Stream(ctx context.Context, w http.ResponseWriter, model, prompt, requestID string) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("ResponseWriter does not support http.Flusher")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	body := fmt.Sprintf(`{"model":%q,"prompt":%q,"stream":true}`, model, prompt)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.upstreamURL+"/api/generate", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("building upstream request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		writeSSEErr(w, flusher, "upstream_error", err.Error())
		return fmt.Errorf("upstream request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		writeSSEErr(w, flusher, "upstream_error", string(msg))
		return fmt.Errorf("upstream returned %d", resp.StatusCode)
	}

	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(*bufPtr, sseMaxLine)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := scanner.Bytes()
		if len(line) == 0 { continue }

		var chunk ollamaChunk
		if json.Unmarshal(line, &chunk) != nil { continue }

		var fr *string
		if chunk.Done { s2 := "stop"; fr = &s2 }

		payload, _ := json.Marshal(openAIChunk{
			ID: requestID, Object: "text_completion",
			Created: time.Now().Unix(), Model: model,
			Choices: []choice{{Text: chunk.Response, FinishReason: fr}},
		})
		fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()

		if chunk.Done {
			fmt.Fprintf(w, "data: [DONE]\n\n")
			flusher.Flush()
			break
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("scanner: %w", err)
	}
	return nil
}

func writeSSEErr(w http.ResponseWriter, f http.Flusher, code, msg string) {
	fmt.Fprintf(w, "data: {\"error\":{\"code\":%q,\"message\":%q}}\n\n", code, msg)
	f.Flush()
}
