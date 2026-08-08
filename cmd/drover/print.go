package main

import (
	"fmt"
	"io"
)

// discard write failures to stdout/stderr — CLI exit codes carry the outcome.
func cliPrintf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func cliPrintln(w io.Writer, args ...any) {
	_, _ = fmt.Fprintln(w, args...)
}
