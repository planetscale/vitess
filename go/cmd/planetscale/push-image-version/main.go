package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

const (
	hmacHeader = "X-Signature"
)

type PushRequest struct {
	VitessImageVersion VitessImageVersion `json:"vitess_image_version"`
}

type VitessImageVersion struct {
	CommitSha  string `json:"commit_sha"`
	CommitDate string `json:"commit_date"`
	Major      string `json:"major_version"`
	Minor      string `json:"minor_version"`
	Patch      string `json:"patch_version"`
}

func main() {
	if err := realMain(); err != nil {
		log.Fatalf("push-image-version failure: %q", err)
	}
}

func realMain() error {
	version := flag.String("version", "", "the output of vttablet --version")
	hmacKey := flag.String("hmac-key", "", "the hmac secret key")
	apibbURL := flag.String("url", "http://admin.pscaledev.com:3000/api/external-admin/vitess-image-versions", "url to push the vitess image version")

	flag.Parse()

	v, err := parseVersion(*version)
	if err != nil {
		return err
	}

	return pushVersion(*apibbURL, *hmacKey, *v)
}

func pushVersion(apiURL, hmacKey string, version VitessImageVersion) error {
	if hmacKey == "" {
		return errors.New("hmacKey is required")
	}

	out, err := json.Marshal(&PushRequest{VitessImageVersion: version})
	if err != nil {
		return err
	}

	method := "POST"
	req, err := http.NewRequest(method, apiURL, bytes.NewReader(out))
	if err != nil {
		return err
	}

	u, err := url.Parse(apiURL)
	if err != nil {
		return fmt.Errorf("couldn't parse URL %q: %s", apiURL, err)
	}

	rh := requestHMAC{
		method:    method,
		path:      u.Path,
		payload:   string(out),
		timestamp: time.Now(),
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add(hmacHeader, rh.generate(hmacKey))

	c := &http.Client{Timeout: 15 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

type requestHMAC struct {
	method    string
	path      string
	payload   string
	timestamp time.Time
}

// computeHMAC returns a computed HMAC value for the given timestamp and key.
func (r *requestHMAC) generate(key string) string {
	mac := hmac.New(sha256.New, []byte(key))

	// signature: "GET /external/authenticated 1234567 {\"foo\":\"bar\"}")
	// https://github.com/planetscale/api-bb/blob/main/lib/hmac_helpers.rb#L78
	fmt.Fprintf(mac,
		"%s %s %d %s",
		r.method, r.path, r.timestamp.Unix(), r.payload,
	)

	return fmt.Sprintf("%d.%s", r.timestamp.Unix(), hex.EncodeToString(mac.Sum(nil)))
}

// Parses the following output of "vttablet --version".
// TODO(fatih): replace this by when we introduce --version-json flag to vt
// components.
func parseVersion(output string) (*VitessImageVersion, error) {
	if output == "" {
		return nil, errors.New("vttablet --version output is empty")
	}
	// see go/vt/servenv/buildinfo.go for the pattern
	// we only care about couple of things, hence we use the most minimal regexp match
	re := regexp.MustCompile(`^Version: (?P<version>.*) \(Git revision (?P<git_sha>.*) branch \'(?P<branch>.*)\'\) built on (?P<build_date>.*) by.*$`)

	result := re.FindAllStringSubmatch(output, -1)
	if len(result) == 0 {
		// no matches
		return nil, fmt.Errorf("couldn't parse vttablet version output: %q", output)
	}

	names := re.SubexpNames()
	matches := map[string]string{}
	for i, n := range result[0] {
		matches[names[i]] = n
	}

	ver, ok := matches["version"]
	if !ok {
		return nil, fmt.Errorf("couldn't parse vttablet version: %q", matches)
	}

	sver, err := parseSemver(ver)
	if err != nil {
		return nil, err
	}

	commitDate, ok := matches["build_date"]
	if !ok {
		return nil, fmt.Errorf("couldn't parse vttablet build date: %q", matches)
	}

	commitSHA, ok := matches["git_sha"]
	if !ok {
		return nil, fmt.Errorf("couldn't parse vttablet git sha: %q", matches)
	}

	return &VitessImageVersion{
		CommitDate: commitDate,
		CommitSha:  commitSHA,
		Major:      sver.major,
		Minor:      sver.minor,
		Patch:      sver.patch,
	}, nil
}

type semver struct {
	major string
	minor string
	patch string
}

func parseSemver(s string) (*semver, error) {
	// official regex for parsing a semver. We don't use a basic split function
	// because it's more work (i.e: 15.0.0-snapshot). The regex is future-proof
	// and also doesn't require any extra dependency.
	// https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
	re := regexp.MustCompile(`^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)

	result := re.FindAllStringSubmatch(s, -1)
	if len(result) == 0 {
		// no matches
		return nil, fmt.Errorf("couldn't parse vttablet semver: %q", s)
	}

	names := re.SubexpNames()
	matches := map[string]string{}
	for i, n := range result[0] {
		matches[names[i]] = n
	}

	major, ok := matches["major"]
	if !ok {
		return nil, fmt.Errorf("couldn't parse semver major: %q", matches)
	}

	minor, ok := matches["minor"]
	if !ok {
		return nil, fmt.Errorf("couldn't parse semver minor: %q", matches)
	}

	patch, ok := matches["patch"]
	if !ok {
		return nil, fmt.Errorf("couldn't parse semver patch: %q", matches)
	}

	return &semver{
		major: major,
		minor: minor,
		patch: patch,
	}, nil
}
