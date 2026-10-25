// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import "testing"

// The key and address come from TEEPIN_API_KEY and TEEPIN_BASE_URL when the
// Config leaves them empty, as OpenAI's SDKs read OPENAI_API_KEY and
// OPENAI_BASE_URL; a value given always wins; production is the default.
func TestNewClient_ReadsTheEnvironment(t *testing.T) {
	t.Setenv("TEEPIN_API_KEY", "tpk_from_env")
	t.Setenv("TEEPIN_BASE_URL", "https://dev-api.example.test/")

	c := NewClient(Config{})
	if c.apiKey != "tpk_from_env" || c.baseURL != "https://dev-api.example.test" {
		t.Errorf("from the environment: key %q base %q", c.apiKey, c.baseURL)
	}

	c = NewClient(Config{APIKey: "tpk_given", BaseURL: "https://given.example.test"})
	if c.apiKey != "tpk_given" || c.baseURL != "https://given.example.test" {
		t.Errorf("given values must win: key %q base %q", c.apiKey, c.baseURL)
	}
}

func TestNewClient_DefaultsWithoutTheEnvironment(t *testing.T) {
	t.Setenv("TEEPIN_API_KEY", "")
	t.Setenv("TEEPIN_BASE_URL", "")

	c := NewClient(Config{})
	if c.apiKey != "" || c.baseURL != DefaultBaseURL {
		t.Errorf("neither set: key %q base %q, want no key and %s", c.apiKey, c.baseURL, DefaultBaseURL)
	}
}
