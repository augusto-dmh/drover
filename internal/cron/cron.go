// Package cron parses unix 5-field expressions and @every durations.
// The grammar is owned here so it can be fuzzed without a third-party
// module (ADR-0004).
package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidSpec is returned when Parse or ParseIn cannot read a spec.
var ErrInvalidSpec = errors.New("cron: invalid spec")

// A Schedule reports the next instant a spec fires.
type Schedule interface {
	// Next returns the earliest instant strictly after t that matches.
	Next(t time.Time) time.Time
}

// Parse reads a spec in UTC.
func Parse(spec string) (Schedule, error) {
	return ParseIn(spec, time.UTC)
}

// ParseIn reads a spec whose 5-field times are interpreted in loc.
// A nil loc is UTC. @every schedules stay Unix-epoch aligned regardless
// of loc.
func ParseIn(spec string, loc *time.Location) (Schedule, error) {
	if loc == nil {
		loc = time.UTC
	}
	fields := strings.Fields(spec)
	if len(fields) > 0 && fields[0] == "@every" {
		return parseEvery(fields[1:])
	}
	if len(fields) != 5 {
		return nil, fmt.Errorf("%w: want 5 fields, got %d", ErrInvalidSpec, len(fields))
	}
	minute, err := parseField(fields[0], 0, 59, false)
	if err != nil {
		return nil, fmt.Errorf("%w: minute: %w", ErrInvalidSpec, err)
	}
	hour, err := parseField(fields[1], 0, 23, false)
	if err != nil {
		return nil, fmt.Errorf("%w: hour: %w", ErrInvalidSpec, err)
	}
	dom, err := parseField(fields[2], 1, 31, false)
	if err != nil {
		return nil, fmt.Errorf("%w: day of month: %w", ErrInvalidSpec, err)
	}
	month, err := parseField(fields[3], 1, 12, false)
	if err != nil {
		return nil, fmt.Errorf("%w: month: %w", ErrInvalidSpec, err)
	}
	dow, err := parseField(fields[4], 0, 7, true)
	if err != nil {
		return nil, fmt.Errorf("%w: day of week: %w", ErrInvalidSpec, err)
	}
	return &cronSchedule{
		minute:  minute,
		hour:    hour,
		dom:     dom,
		month:   month,
		dow:     dow,
		domStar: fields[2] == "*",
		dowStar: fields[4] == "*",
		loc:     loc,
	}, nil
}

func parseEvery(rest []string) (Schedule, error) {
	if len(rest) == 0 {
		return nil, fmt.Errorf("%w: empty @every duration", ErrInvalidSpec)
	}
	d, err := time.ParseDuration(strings.Join(rest, " "))
	if err != nil {
		return nil, fmt.Errorf("%w: @every duration: %w", ErrInvalidSpec, err)
	}
	if d <= 0 {
		return nil, fmt.Errorf("%w: @every duration must be positive", ErrInvalidSpec)
	}
	return everySchedule{d: d}, nil
}

type field struct {
	bits uint64
}

func (f field) has(v int) bool {
	if v < 0 || v > 63 {
		return false
	}
	return f.bits&(1<<v) != 0
}

func parseField(tok string, minVal, maxVal int, wrap7 bool) (field, error) {
	if tok == "" {
		return field{}, fmt.Errorf("empty field")
	}
	var f field
	for part := range strings.SplitSeq(tok, ",") {
		if part == "" {
			return field{}, fmt.Errorf("empty list item")
		}
		if err := addPart(&f, part, minVal, maxVal, wrap7); err != nil {
			return field{}, err
		}
	}
	if f.bits == 0 {
		return field{}, fmt.Errorf("no valid values")
	}
	return f, nil
}

func addPart(f *field, part string, minVal, maxVal int, wrap7 bool) error {
	rangeTok, stepTok, stepped := strings.Cut(part, "/")
	step := 1
	if stepped {
		n, err := strconv.Atoi(stepTok)
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid step %q", stepTok)
		}
		step = n
	}
	start, end, err := parseRange(rangeTok, minVal, maxVal)
	if err != nil {
		return err
	}
	for v := start; v <= end; v += step {
		bit := v
		if wrap7 && bit == 7 {
			bit = 0
		}
		f.bits |= 1 << bit
	}
	return nil
}

func parseRange(tok string, minVal, maxVal int) (start, end int, err error) {
	if tok == "*" {
		return minVal, maxVal, nil
	}
	low, high, ranged := strings.Cut(tok, "-")
	start, err = strconv.Atoi(low)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid value %q", tok)
	}
	if !ranged {
		end = start
	} else {
		end, err = strconv.Atoi(high)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range %q", tok)
		}
	}
	if start < minVal || end > maxVal || start > end {
		return 0, 0, fmt.Errorf("value %q out of range %d-%d", tok, minVal, maxVal)
	}
	return start, end, nil
}

const maxScanMinutes = 5 * 366 * 24 * 60

type cronSchedule struct {
	minute, hour, dom, month, dow field
	domStar, dowStar              bool
	loc                           *time.Location
}

func (s *cronSchedule) Next(t time.Time) time.Time {
	t = t.In(s.loc)
	year, month, day := t.Date()
	hour, min, _ := t.Clock()
	t = time.Date(year, month, day, hour, min, 0, 0, s.loc).Add(time.Minute)
	for range maxScanMinutes {
		if s.matches(t) {
			return t
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}
}

func (s *cronSchedule) matches(t time.Time) bool {
	if !s.minute.has(t.Minute()) || !s.hour.has(t.Hour()) || !s.month.has(int(t.Month())) {
		return false
	}
	dom := s.dom.has(t.Day())
	dow := s.dow.has(int(t.Weekday()))
	if !s.domStar && !s.dowStar {
		return dom || dow
	}
	return dom && dow
}

type everySchedule struct {
	d time.Duration
}

func (s everySchedule) Next(t time.Time) time.Time {
	next := t.UTC().Truncate(s.d).Add(s.d)
	for !next.After(t) {
		next = next.Add(s.d)
	}
	return next
}
