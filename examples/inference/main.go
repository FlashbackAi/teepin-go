// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Command inference shows Teepin Inference from Go: list the models, then
// stream an answer and show a confidential model's verification.
//
//	TEEPIN_API_KEY=tpk_... go run ./examples/inference teepin/glm-5.3 "Explain TLS briefly"
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/FlashbackAi/teepin-go/teepin"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: inference MODEL PROMPT")
	}
	// Reads TEEPIN_API_KEY, and TEEPIN_BASE_URL (default https://api.teepin.com).
	client := teepin.NewClient(teepin.Config{})
	ctx := context.Background()

	models, err := client.Inference.ListModels(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range models {
		fmt.Printf("%-32s $%.2f/$%.2f per M tokens confidential=%v\n", m.ID,
			m.Pricing.InputPerMillionTokens, m.Pricing.OutputPerMillionTokens, m.Confidential)
	}

	// Reasoning models spend the token budget thinking first: a short answer
	// turns thinking off and leaves room.
	stream, err := client.Inference.ChatStream(ctx, teepin.ChatRequest{
		Model:              os.Args[1],
		Messages:           []teepin.Message{teepin.UserMessage(os.Args[2])},
		MaxTokens:          300,
		ChatTemplateKwargs: map[string]any{"enable_thinking": false},
		StreamOptions:      &teepin.StreamOptions{IncludeUsage: true},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()
	if a := stream.Meta.Attestation; a != "" && a != teepin.AttestationVerified {
		fmt.Fprintln(os.Stderr, "warning: enclave verification:", a)
	}
	wrote, finish := false, ""
	for stream.Next() {
		chunk := stream.Chunk()
		if text := chunk.Text(); text != "" {
			fmt.Print(text)
			wrote = true
		}
		if len(chunk.Choices) > 0 && chunk.Choices[0].FinishReason != "" {
			finish = chunk.Choices[0].FinishReason
		}
	}
	fmt.Println()
	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
	if !wrote && finish == "length" {
		fmt.Println("(no answer: the model used its whole token budget thinking; raise MaxTokens or turn thinking off)")
	}
	if u := stream.Usage(); u != nil {
		fmt.Printf("tokens: %d in, %d out\n", u.PromptTokens, u.CompletionTokens)
	}
}
