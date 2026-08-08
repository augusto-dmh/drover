package main

import (
	"fmt"
	"strings"
)

// globalConfig holds flags that apply to every command.
type globalConfig struct {
	database string
	json     bool
	version  bool
	help     bool
}

// peelGlobals pulls known global flags from anywhere in args so both
// `drover --json stats` and `drover stats --json` work. Unknown flags
// and positional tokens stay in the remainder for the subcommand.
func peelGlobals(args []string) (globalConfig, []string, error) {
	var cfg globalConfig
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			rest = append(rest, args[i+1:]...)
			return cfg, rest, nil
		case a == "--json":
			cfg.json = true
		case a == "--version":
			cfg.version = true
		case a == "-h", a == "--help":
			cfg.help = true
		case a == "--database":
			if i+1 >= len(args) {
				return cfg, nil, fmt.Errorf("--database requires a value")
			}
			i++
			cfg.database = args[i]
		case strings.HasPrefix(a, "--database="):
			cfg.database = strings.TrimPrefix(a, "--database=")
			if cfg.database == "" {
				return cfg, nil, fmt.Errorf("--database requires a value")
			}
		default:
			rest = append(rest, a)
		}
	}
	return cfg, rest, nil
}

// resolveDSN returns the Postgres URL from --database or DATABASE_URL.
func resolveDSN(flagValue string, getenv func(string) string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if getenv == nil {
		return "", fmt.Errorf("database URL required: pass --database or set DATABASE_URL")
	}
	if v := getenv("DATABASE_URL"); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("database URL required: pass --database or set DATABASE_URL")
}
