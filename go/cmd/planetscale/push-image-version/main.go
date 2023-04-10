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
	v := VitessImageVersion{}
	flag.StringVar(&v.CommitSha, "commit-sha", "", "the commit sha of the build")
	flag.StringVar(&v.CommitDate, "commit-date", "", "the commit date of the build")
	flag.StringVar(&v.Major, "major", "", "the major version of the build")
	flag.StringVar(&v.Minor, "minor", "", "the minor version of the build")
	flag.StringVar(&v.Patch, "patch", "", "the patch version of the build")
	hmacKey := flag.String("hmac-key", "", "the hmac secret key")
	apibbURL := flag.String("url", "http://admin.pscaledev.com:3000/api/external-admin/vitess-image-versions", "url to push the vitess image version")

	flag.Parse()

	return pushVersion(*apibbURL, *hmacKey, v)
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
