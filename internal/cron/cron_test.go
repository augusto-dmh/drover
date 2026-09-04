package cron

import (
	"fmt"
	"testing"
	"time"
	_ "time/tzdata"
)

func mustParse(t *testing.T, spec string) Schedule {
	t.Helper()
	s, err := Parse(spec)
	if err != nil {
		t.Fatalf("Parse(%q): %v", spec, err)
	}
	return s
}

func mustParseIn(t *testing.T, spec string, loc *time.Location) Schedule {
	t.Helper()
	s, err := ParseIn(spec, loc)
	if err != nil {
		t.Fatalf("ParseIn(%q): %v", spec, err)
	}
	return s
}

func ny(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	return loc
}

func TestParseNextFiveField(t *testing.T) {
	t.Parallel()

	utc := time.UTC
	loc := ny(t)

	tests := []struct {
		name    string
		spec    string
		parseIn bool
		loc     *time.Location
		from    time.Time
		want    time.Time
	}{
		{
			name: "hourly at minute zero after a half hour",
			spec: "0 * * * *",
			from: time.Date(2026, 1, 15, 10, 30, 0, 0, utc),
			want: time.Date(2026, 1, 15, 11, 0, 0, 0, utc),
		},
		{
			name: "hourly at minute zero is strictly after an exact hit",
			spec: "0 * * * *",
			from: time.Date(2026, 1, 15, 11, 0, 0, 0, utc),
			want: time.Date(2026, 1, 15, 12, 0, 0, 0, utc),
		},
		{
			name: "every five minutes from just after a tick",
			spec: "*/5 * * * *",
			from: time.Date(2026, 1, 15, 10, 0, 0, 0, utc),
			want: time.Date(2026, 1, 15, 10, 5, 0, 0, utc),
		},
		{
			name: "every five minutes from between ticks",
			spec: "*/5 * * * *",
			from: time.Date(2026, 1, 15, 10, 4, 0, 0, utc),
			want: time.Date(2026, 1, 15, 10, 5, 0, 0, utc),
		},
		{
			name: "every five minutes is strictly after an exact hit",
			spec: "*/5 * * * *",
			from: time.Date(2026, 1, 15, 10, 5, 0, 0, utc),
			want: time.Date(2026, 1, 15, 10, 10, 0, 0, utc),
		},
		{
			name: "list of minutes",
			spec: "0,30 * * * *",
			from: time.Date(2026, 1, 15, 10, 15, 0, 0, utc),
			want: time.Date(2026, 1, 15, 10, 30, 0, 0, utc),
		},
		{
			name: "range of minutes",
			spec: "10-12 * * * *",
			from: time.Date(2026, 1, 15, 10, 12, 0, 0, utc),
			want: time.Date(2026, 1, 15, 11, 10, 0, 0, utc),
		},
		{
			name: "stepped range of minutes",
			spec: "0-10/2 * * * *",
			from: time.Date(2026, 1, 15, 10, 5, 0, 0, utc),
			want: time.Date(2026, 1, 15, 10, 6, 0, 0, utc),
		},
		{
			name:    "nil location interprets fields in UTC",
			spec:    "0 12 * * *",
			parseIn: true,
			loc:     nil,
			from:    time.Date(2026, 6, 15, 0, 0, 0, 0, utc),
			want:    time.Date(2026, 6, 15, 12, 0, 0, 0, utc),
		},
		{
			name:    "non-nil location interprets fields in that location",
			spec:    "0 12 * * *",
			parseIn: true,
			loc:     loc,
			from:    time.Date(2026, 6, 15, 0, 0, 0, 0, utc),
			want:    time.Date(2026, 6, 15, 12, 0, 0, 0, loc),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sched Schedule
			if tc.parseIn {
				sched = mustParseIn(t, tc.spec, tc.loc)
			} else {
				sched = mustParse(t, tc.spec)
			}
			got := sched.Next(tc.from)
			if !got.Equal(tc.want) {
				t.Errorf("Next(%v) = %v, want %v", tc.from, got.In(time.UTC), tc.want.In(time.UTC))
			}
			if !got.After(tc.from) {
				t.Errorf("Next(%v) = %v, want an instant strictly after from", tc.from, got)
			}
		})
	}
}

func TestParseNextEvery(t *testing.T) {
	t.Parallel()

	epoch := time.Unix(0, 0).UTC()
	d := 30 * time.Second

	tests := []struct {
		name string
		from time.Time
		want time.Time
	}{
		{
			name: "from epoch",
			from: epoch,
			want: epoch.Add(d),
		},
		{
			name: "between ticks",
			from: epoch.Add(45 * time.Second),
			want: epoch.Add(60 * time.Second),
		},
		{
			name: "exact tick is strictly after",
			from: epoch.Add(d),
			want: epoch.Add(2 * d),
		},
	}

	sched := mustParse(t, "@every 30s")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := sched.Next(tc.from)
			if !got.Equal(tc.want) {
				t.Errorf("Next(%v) = %v, want %v", tc.from, got, tc.want)
			}
			if !got.After(tc.from) {
				t.Errorf("Next(%v) = %v, want an instant strictly after from", tc.from, got)
			}
			if got.UnixNano()%d.Nanoseconds() != 0 {
				t.Errorf("Next(%v) = %v is not aligned to Unix-epoch multiples of %s", tc.from, got, d)
			}
		})
	}
}

func TestEveryStaysEpochAlignedInNonUTC(t *testing.T) {
	t.Parallel()

	loc := ny(t)
	d := 45 * time.Minute
	sched := mustParseIn(t, "@every 45m", loc)
	from := time.Date(2026, 6, 15, 12, 0, 0, 0, loc)

	got := sched.Next(from)

	want := from.UTC().Truncate(d).Add(d)
	for !want.After(from) {
		want = want.Add(d)
	}
	if !got.Equal(want) {
		t.Errorf("Next(%v) = %v, want epoch-aligned %v", from, got.UTC(), want.UTC())
	}
	if got.UnixNano()%d.Nanoseconds() != 0 {
		t.Errorf("Next(%v) = %v is not aligned to Unix-epoch multiples of %s", from, got, d)
	}

	// Local-wall alignment (midnight in loc, then Truncate) is a
	// different instant here; landing on it would mean Next ignored UTC.
	midnight := time.Date(2026, 6, 15, 0, 0, 0, 0, loc)
	elapsed := from.Sub(midnight)
	localWall := midnight.Add(elapsed.Truncate(d)).Add(d)
	if got.Equal(localWall) {
		t.Errorf("Next(%v) matched local-wall alignment %v, want epoch alignment %v", from, localWall.UTC(), want.UTC())
	}
}

func TestSundayZeroAndSeven(t *testing.T) {
	t.Parallel()

	// 2026-01-17 is a Saturday; 2026-01-18 is Sunday.
	from := time.Date(2026, 1, 17, 15, 0, 0, 0, time.UTC)
	want := time.Date(2026, 1, 18, 0, 0, 0, 0, time.UTC)

	for _, spec := range []string{"0 0 * * 0", "0 0 * * 7"} {
		t.Run(spec, func(t *testing.T) {
			t.Parallel()
			got := mustParse(t, spec).Next(from)
			if !got.Equal(want) {
				t.Errorf("Next(%v) = %v, want Sunday %v", from, got, want)
			}
			if got.Weekday() != time.Sunday {
				t.Errorf("Next(%v) weekday = %v, want Sunday", from, got.Weekday())
			}
		})
	}
}

func TestNextStrictlyAfter(t *testing.T) {
	t.Parallel()

	froms := []time.Time{
		time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 15, 10, 0, 0, 1, time.UTC),
		time.Unix(0, 0).UTC(),
		time.Unix(0, 0).UTC().Add(30 * time.Second),
	}
	specs := []string{"0 * * * *", "*/5 * * * *", "* * * * *", "@every 30s", "0 0 * * 0", "0 0 * * 7"}

	for _, spec := range specs {
		for _, from := range froms {
			t.Run(fmt.Sprintf("%s from %s", spec, from.Format(time.RFC3339Nano)), func(t *testing.T) {
				t.Parallel()
				got := mustParse(t, spec).Next(from)
				if got.Equal(from) {
					t.Errorf("Next(%v) = %v, must not equal t", from, got)
				}
				if got.Before(from) {
					t.Errorf("Next(%v) = %v, must not be before t", from, got)
				}
				if !got.After(from) {
					t.Errorf("Next(%v) = %v, want After(t)", from, got)
				}
			})
		}
	}
}

func TestParseInvalidSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec string
	}{
		{name: "empty", spec: ""},
		{name: "four fields", spec: "0 * * *"},
		{name: "six fields", spec: "0 0 * * * *"},
		{name: "minute out of range", spec: "60 * * * *"},
		{name: "hour out of range", spec: "0 24 * * *"},
		{name: "dom out of range", spec: "0 0 32 * *"},
		{name: "dom zero", spec: "0 0 0 * *"},
		{name: "month out of range", spec: "0 0 * 13 *"},
		{name: "month zero", spec: "0 0 * 0 *"},
		{name: "dow out of range", spec: "0 0 * * 8"},
		{name: "empty every duration", spec: "@every"},
		{name: "blank every duration", spec: "@every "},
		{name: "zero every duration", spec: "@every 0s"},
		{name: "negative every duration", spec: "@every -1s"},
		{name: "unknown at token", spec: "@hourly"},
		{name: "named day", spec: "0 0 * * MON"},
		{name: "named month", spec: "0 0 * JAN *"},
		{name: "quartz question mark", spec: "0 0 * * ?"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Parse(%q) panicked: %v", tc.spec, r)
				}
			}()
			_, err := Parse(tc.spec)
			if err == nil {
				t.Fatalf("Parse(%q) succeeded, want error", tc.spec)
			}
		})
	}
}

func TestSpringForwardYieldsLaterInstant(t *testing.T) {
	t.Parallel()

	loc := ny(t)
	// 2026-03-08 02:00 EST is skipped; clocks jump to 03:00 EDT.
	from := time.Date(2026, 3, 8, 1, 59, 0, 0, loc)

	everyMinute := mustParseIn(t, "* * * * *", loc).Next(from)
	if !everyMinute.After(from) {
		t.Fatalf("every-minute Next(%v) = %v, want a later instant", from, everyMinute)
	}
	wantMinute := time.Date(2026, 3, 8, 3, 0, 0, 0, loc)
	if !everyMinute.Equal(wantMinute) {
		t.Errorf("every-minute Next(%v) = %v, want %v", from, everyMinute, wantMinute)
	}

	fromBeforeGap := time.Date(2026, 3, 8, 1, 0, 0, 0, loc)
	skippedHour := mustParseIn(t, "30 2 * * *", loc).Next(fromBeforeGap)
	if !skippedHour.After(fromBeforeGap) {
		t.Fatalf("2:30 Next(%v) = %v, want a later instant", fromBeforeGap, skippedHour)
	}
	wantNextDay := time.Date(2026, 3, 9, 2, 30, 0, 0, loc)
	if !skippedHour.Equal(wantNextDay) {
		t.Errorf("2:30 Next(%v) = %v, want next-day %v", fromBeforeGap, skippedHour, wantNextDay)
	}
}

func TestNextUnsatisfiableDOMReturnsZero(t *testing.T) {
	t.Parallel()
	s := mustParse(t, "0 0 31 2 *")
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := s.Next(from)
	if !got.IsZero() {
		t.Errorf("Next(%v) = %v, want zero", from, got)
	}
}

func FuzzParse(f *testing.F) {
	for _, spec := range []string{
		"0 * * * *",
		"*/5 * * * *",
		"0,30 * * * *",
		"0-5 * * * *",
		"0-10/2 * * * *",
		"0 0 * * 0",
		"0 0 * * 7",
		"* * * * *",
		"@every 30s",
		"@every 1h",
		"",
		"0 * * *",
		"60 * * * *",
		"@every",
		"@every 0s",
		"@every -1s",
		"@hourly",
		"0 0 * * MON",
		"0 0 * * ?",
	} {
		f.Add(spec)
	}
	f.Fuzz(func(t *testing.T, spec string) {
		s, err := Parse(spec)
		if err != nil {
			return
		}
		_ = s.Next(time.Unix(0, 0).UTC())
	})
}
