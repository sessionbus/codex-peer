// SPDX-License-Identifier: MIT

package codex

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// bindTransferPaths makes a transfer action's local paths absolute from the
// Codex thread's working directory. The SDK opens a path as given, so a
// relative one would otherwise resolve against this process's directory,
// which is not the thread's. The path is joined, not cleaned, so the native
// filesystem still resolves symbolic links and "..". A relative path with no
// known working directory is refused; every other field, and every other
// action, passes unchanged to the SDK's own validation.
func bindTransferPaths(action string, args json.RawMessage, cwd string) (json.RawMessage, error) {
	var keys []string
	switch action {
	case "transfer.offer":
		keys = []string{"source"}
	case "transfer.request":
		keys = []string{"destination"}
	case "transfer.accept":
		keys = []string{"source", "destination"}
	default:
		return args, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(args, &fields) != nil || fields == nil {
		return args, nil
	}
	// The SDK decodes these fields as Go's encoding/json does, matching a key
	// to a field case-insensitively (also "ſ" for "s"), so another spelling
	// of a path key would reach it unbound. Such a key is refused, alone or
	// beside the documented one, as the SDK refuses a field it does not know.
	for key := range fields {
		for _, name := range keys {
			if key != name && strings.EqualFold(key, name) {
				return nil, fmt.Errorf("json: unknown field %q", key)
			}
		}
	}
	changed := false
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var path string
		if json.Unmarshal(raw, &path) != nil || path == "" || filepath.IsAbs(path) {
			continue
		}
		if !filepath.IsAbs(cwd) {
			return nil, errors.New("transfer " + key + " must be an absolute path: the thread's working directory is unknown")
		}
		encoded, err := json.Marshal(strings.TrimSuffix(cwd, string(filepath.Separator)) + string(filepath.Separator) + path)
		if err != nil {
			return nil, err
		}
		fields[key] = encoded
		changed = true
	}
	if !changed {
		return args, nil
	}
	return json.Marshal(fields)
}
