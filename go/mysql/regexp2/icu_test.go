package regexp2

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql/regexp2/syntax"
	"vitess.io/vitess/go/slices2"
)

var ErrSkip = errors.New("ignored test")

type TestPattern struct {
	Line   string
	Lineno int

	Pattern string
	Flags   syntax.RegexOptions
	Options struct {
		FindCount int
		MatchOnly bool
		MustError bool
	}
	Input  string
	Groups []TestGroup
}

type TestGroup struct {
	Start, End int
}

var parsePattern = regexp.MustCompile(`<(/?)(r|[0-9]+)>`)

func (tp *TestPattern) parseFlags(line string) (string, error) {
	for len(line) > 0 {
		switch line[0] {
		case '"', '\'', '/':
			return line, nil
		case ' ', '\t':
		case 'i':
			tp.Flags |= syntax.IgnoreCase
		case 'x':
			tp.Flags |= syntax.IgnorePatternWhitespace
		case 's':
			tp.Flags |= syntax.Singleline
		case 'm':
			tp.Flags |= syntax.Multiline
		case 'D':
			tp.Flags |= syntax.UnixLines
		case 'Q':
			tp.Flags |= syntax.Literal
		case '2', '3', '4', '5', '6', '7', '8', '9':
			tp.Options.FindCount = int(line[0] - '0')
		case 'G':
			tp.Options.MatchOnly = true
		case 'E':
			tp.Options.MustError = true
		case 'L', 'M':
			return "", ErrSkip
		case 'z', 'Z', 'y', 'Y', 'v':
			// ignored for now
		default:
			return "", ErrSkip
		}
		line = line[1:]
	}
	return "", io.ErrUnexpectedEOF
}

func (tp *TestPattern) parseMatch(quotedInput string) error {
	input, err := syntax.Unescape(quotedInput)
	if err != nil {
		return fmt.Errorf("failed to unquote input: %w", err)
	}

	var detagged strings.Builder
	var last int

	m := parsePattern.FindAllStringSubmatchIndex(input, -1)
	for _, g := range m {
		detagged.WriteString(input[last:g[0]])
		last = g[1]

		closing := input[g[2]:g[3]] == "/"
		groupNum := input[g[4]:g[5]]
		if groupNum == "r" {
			return ErrSkip
		} else {
			num, err := strconv.Atoi(groupNum)
			if err != nil {
				return fmt.Errorf("bad group number %q: %v", groupNum, err)
			}

			if num >= len(tp.Groups) {
				grp := make([]TestGroup, num+1)
				copy(grp, tp.Groups)
				tp.Groups = grp
			}

			if closing {
				tp.Groups[num].End = detagged.Len()
			} else {
				tp.Groups[num].Start = detagged.Len()
			}
		}
	}

	detagged.WriteString(input[last:])
	tp.Input = detagged.String()
	return nil
}

func ParseTestFile(t testing.TB, filename string) []TestPattern {
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("failed to open test data: %v", err)
	}

	defer f.Close()
	scanner := bufio.NewScanner(f)
	var lineno int
	var patterns []TestPattern

	error := func(err error) {
		if err == ErrSkip {
			return
		}
		t.Errorf("Parse error: %v\n%03d: %s", err, lineno, scanner.Text())
	}

	for scanner.Scan() {
		lineno++
		line := scanner.Text()
		line = strings.TrimSpace(line)

		if len(line) == 0 || line[0] == '#' {
			continue
		}

		var tp TestPattern
		tp.Line = line
		tp.Lineno = lineno

		idx := strings.IndexByte(line[1:], line[0])

		tp.Pattern = line[1 : idx+1]
		line, err = tp.parseFlags(line[idx+2:])
		if err != nil {
			error(err)
			continue
		}

		idx = strings.IndexByte(line[1:], line[0])
		err = tp.parseMatch(line[1 : idx+1])
		if err != nil {
			error(err)
			continue
		}

		line = line[idx+2:]
		patterns = append(patterns, tp)
	}

	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return patterns
}

func (tp *TestPattern) fail(t testing.TB, msg string, args ...any) bool {
	msg = fmt.Sprintf(msg, args...)
	t.Errorf("%s (in line %d)\nregexp: %s\ninput: %q\noriginal: %s", msg, tp.Lineno, tp.Pattern, tp.Input, tp.Line)
	return false
}

func (tp *TestPattern) Test(t testing.TB) bool {
	re, err := Compile(tp.Pattern, tp.Flags)
	if err != nil {
		if tp.Options.MustError {
			return true
		}

		return tp.fail(t, "unexpected parser failure: %v", err)
	}
	if tp.Options.MustError {
		return tp.fail(t, "parse failure expected")
	}

	m, err := re.FindStringMatch(tp.Input)
	if err != nil {
		return tp.fail(t, "error when matching: %v", err)
	}

	if tp.Options.FindCount > 0 {
		for i := 1; i < tp.Options.FindCount; i++ {
			m, err = re.FindNextMatch(m)
			if err != nil {
				return tp.fail(t, "error when matching (%d iteration): %v", i, err)
			}
		}
	}

	if m == nil && len(tp.Groups) > 0 {
		return tp.fail(t, "Match expected, but none found.")
	}
	if m != nil && len(tp.Groups) == 0 {
		return tp.fail(t, "No match expected, but found one at position %d", m.Index)
	}
	if tp.Options.MatchOnly {
		return true
	}

	// TODO

	return true
}

func TestICU(t *testing.T) {
	pats := ParseTestFile(t, "testdata/regextst.txt")

	var valid int

	for _, p := range pats {
		if p.Test(t) {
			valid++
		}
	}

	t.Logf("%d/%d (%.02f)", valid, len(pats), float64(valid)/float64(len(pats)))
}

func TestCornerCases(t *testing.T) {
	var cases = []struct {
		Pattern string
		Input   string
		Flags   syntax.RegexOptions
		Match   bool
	}{
		{`xyz$`, "xyz\n", 0, true},
		{`a*+`, "abbxx", 0, true},
		{`(ABC){1,2}+ABC`, "ABCABCABC", 0, true},
		{`(ABC){2,3}+ABC`, "ABCABCABC", 0, false},
		{`(abc)*+a`, "abcabcabc", 0, false},
		{`(abc)*+a`, "abcabcab", 0, true},
		{`a\N{LATIN SMALL LETTER B}c`, "abc", 0, true},
		{`a.b`, "a\rb", syntax.UnixLines, true},
		{`a.b`, "a\rb", 0, false},
		{`(?d)abc$`, "abc\r", 0, false},
		{`[ \b]`, "b", syntax.Debug, true},
		{`[abcd-\N{LATIN SMALL LETTER G}]+`, "xyz-abcdefghij-", 0, true},
		{`[[abcd]&&[ac]]+`, "bacacd", 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.Pattern, func(t *testing.T) {
			re, err := Compile(tc.Pattern, tc.Flags)
			if err != nil {
				t.Fatal(err)
			}
			m, err := re.FindStringMatch(tc.Input)
			if err != nil {
				t.Fatal(err)
			}

			var match []string
			if m != nil {
				match = slices2.Map(m.Groups(), func(c Group) string {
					return strconv.Quote(c.String())
				})
			}

			if tc.Match != (m != nil) {
				t.Errorf("%q ~= /%s/\n\t%v", tc.Input, tc.Pattern, match)
			}
		})
	}
}
