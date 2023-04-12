package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/servenv"
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
	Major      int64  `json:"major_version"`
	Minor      int64  `json:"minor_version"`
	Patch      int64  `json:"patch_version"`
	PRNumber   *int64 `json:"pr_number,omitempty"`
}

func (v *VitessImageVersion) ImageVersion() string {
	return fmt.Sprintf("%s.%s", v.CommitDate, v.CommitSha)
}

func (v *VitessImageVersion) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\t - Commit SHA: %s\n", v.CommitSha)
	fmt.Fprintf(&b, "\t - Commit Date: %s\n", v.CommitDate)
	fmt.Fprintf(&b, "\t - Vitess Version: %d.%d.%d\n", v.Major, v.Minor, v.Patch)
	if v.PRNumber != nil {
		fmt.Fprintf(&b, "\t - PR Number: %d", *v.PRNumber)
	} else {
		fmt.Fprintf(&b, "\t - PR Number: nil")
	}
	return b.String()
}

func main() {
	if err := realMain(); err != nil {
		log.Fatalf("push-image-version failure: %q", err)
	}
}

func realMain() error {
	log.SetPrefix("push-image-version: ")
	log.SetOutput(os.Stdout)

	prNumber := flag.Int64("pr-number", 0, "the pull request number (if present)")
	commitSHA := flag.String("commit-sha", "", "the commit/build sha of the image")
	commitDate := flag.String("commit-date", "", "the commit/build date of the image")
	hmacKey := flag.String("hmac-key", "", "the hmac secret key")
	apibbURL := flag.String("url", "http://admin.pscaledev.com:3000/api/external-admin/vitess-image-versions", "url to push the vitess image version")

	flag.Parse()

	if *hmacKey == "" {
		return errors.New("hmacKey is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// the vitess version is automatically created by the release scripts and
	// replaced into a file go/vt/servenv/version.go. The servenv package
	// allows us to expose this information.
	vitessVersion, ok := servenv.AppVersion.ToStringMap()["version"]
	if !ok {
		return fmt.Errorf("version is not available in servenv: %q", servenv.AppVersion)
	}

	sver, err := parseSemver(vitessVersion)
	if err != nil {
		return err
	}

	sha := *commitSHA
	if len(sha) > 7 {
		sha = sha[:7]
	}

	v := &VitessImageVersion{
		CommitDate: *commitDate,
		CommitSha:  sha,
		Major:      sver.major,
		Minor:      sver.minor,
		Patch:      sver.patch,
		PRNumber:   prNumber,
	}

	log.Printf("verifying if version already exists: %q", v.ImageVersion())
	_, exists, err := getVersion(ctx, *apibbURL, *hmacKey, v.ImageVersion())
	if err != nil {
		return fmt.Errorf("verifying if version already exists: %w", err)
	}
	if exists {
		log.Printf("Version already exists:\n\n%s", v)
		return nil
	}

	log.Printf("version not found, creating it")
	err = createVersion(ctx, *apibbURL, *hmacKey, *v)
	if err != nil {
		return err
	}

	log.Printf("Published version:\n\n%s", v)
	return nil
}

func getVersion(ctx context.Context, baseURL, hmacKey string, vitessVersion string) (*VitessImageVersion, bool, error) {
	method := "GET"

	getURL := fmt.Sprintf("%s/%s", baseURL, vitessVersion)
	req, err := http.NewRequestWithContext(ctx, method, getURL, nil)
	if err != nil {
		return nil, false, err
	}

	u, err := url.Parse(getURL)
	if err != nil {
		return nil, false, fmt.Errorf("couldn't parse URL %q: %s", getURL, err)
	}

	rh := requestHMAC{
		method:    method,
		path:      u.Path,
		timestamp: time.Now(),
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add(hmacHeader, rh.generate(hmacKey))

	c := &http.Client{Timeout: 15 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, false, nil
	}

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}

	v := &VitessImageVersion{}
	err = json.Unmarshal(out, &v)
	if err != nil {
		return nil, false, err
	}

	return v, true, nil
}

func createVersion(ctx context.Context, baseURL, hmacKey string, version VitessImageVersion) error {
	out, err := json.Marshal(&PushRequest{VitessImageVersion: version})
	if err != nil {
		return err
	}

	method := "POST"
	req, err := http.NewRequestWithContext(ctx, method, baseURL, bytes.NewReader(out))
	if err != nil {
		return err
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("couldn't parse URL %q: %s", baseURL, err)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading body: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status (%s): %s", resp.Status, string(body))
	}

	v := &VitessImageVersion{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		return fmt.Errorf("unmarshaling body: %w", err)
	}

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
	if r.payload == "" {
		fmt.Fprintf(mac, "%s %s %d", r.method, r.path, r.timestamp.Unix())
	} else {
		fmt.Fprintf(mac, "%s %s %d %s", r.method, r.path, r.timestamp.Unix(), r.payload)
	}

	return fmt.Sprintf("%d.%s", r.timestamp.Unix(), hex.EncodeToString(mac.Sum(nil)))
}

type semver struct {
	major int64
	minor int64
	patch int64
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

	major, err := parseMatchAsInt(matches, "major")
	if err != nil {
		return nil, fmt.Errorf("parsing major: %w", err)
	}
	minor, err := parseMatchAsInt(matches, "minor")
	if err != nil {
		return nil, fmt.Errorf("parsing minor: %w", err)
	}
	patch, err := parseMatchAsInt(matches, "patch")
	if err != nil {
		return nil, fmt.Errorf("parsing patch: %w", err)
	}

	return &semver{
		major: major,
		minor: minor,
		patch: patch,
	}, nil
}

func parseMatchAsInt(matches map[string]string, key string) (int64, error) {
	str, ok := matches[key]
	if !ok {
		return -1, fmt.Errorf("no %s found in matches: %q", key, matches)
	}
	v, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("parsing as an integer: %w", err)
	}
	return v, nil
}
