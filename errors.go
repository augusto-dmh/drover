package drover

import "errors"

// ErrInvalidKind is returned by Insert and InsertTx when args.Kind()
// returns an empty string.
var ErrInvalidKind = errors.New("drover: job kind must be non-empty")
