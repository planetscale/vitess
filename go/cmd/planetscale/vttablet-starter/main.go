// vttablet-starter is a process that sits at the root of the vttablet container
// in a tablet Pod and starts the actual vttablet process as soon as all the
// necessary parameters are known. This allows the tablet Pod to be
// pre-provisioned, including provisioning and mounting the tablet's PVC, before
// the tablet is assigned to a specific shard, keyspace, or even cluster.
package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/template"
	"time"

	"vitess.io/vitess/go/exit"
)

func main() {
	defer exit.Recover()

	// We load parameters from env vars instead of command-line flags because we
	// are going to pass all our args to the target command instead of parsing them.
	labelsFilePath := os.Getenv("LABELS_FILE_PATH")
	vttabletPath := os.Getenv("VTTABLET_PATH")
	listenAddress := os.Getenv("LISTEN_ADDRESS")
	if listenAddress == "" {
		listenAddress = ":15000"
	}

	// The args we were passed, excluding our own command path, will become
	// vttablet args when we're ready to run it.
	args := os.Args[1:]

	// Figure out which labels the args are trying to use, so we know which
	// labels we need to wait for before starting vttablet.
	requiredLabelKeys, err := findRequiredLabelKeys(args)
	if err != nil {
		fmt.Printf("findRequiredLabelKeys failed: %v\n", err)
		exit.Return(1)
	}

	// Serve a fake no-op health check handler until we're ready to start vttablet.
	http.HandleFunc("/", func(http.ResponseWriter, *http.Request) {})
	server := &http.Server{
		Addr: listenAddress,
	}
	go server.ListenAndServe()

	fmt.Printf("Waiting for required Pod labels to be assigned: %v\n", strings.Join(requiredLabelKeys, ", "))
	labels, err := waitForLabelKeys(labelsFilePath, requiredLabelKeys)
	if err != nil {
		fmt.Printf("waitForLabelKeys failed: %v\n", err)
		exit.Return(1)
	}

	fmt.Println("Required Pod labels were assigned. Generating final command args...")
	finalArgs, err := generateArgs(args, labels)
	if err != nil {
		fmt.Printf("generateArgs failed: %v\n", err)
		exit.Return(1)
	}

	// Shut down the fake health check handler.
	server.Close()

	fmt.Println("Executing vttablet with final command args...")

	// We use syscall.Exec() instead of exec.Command() so that vttablet replaces
	// this wrapper process instead of being spawned as a child process.
	// That makes things like signal handling easier.
	argv := append([]string{vttabletPath}, finalArgs...)
	if err := syscall.Exec(vttabletPath, argv, syscall.Environ()); err != nil {
		fmt.Printf("Failed to start vttablet: %v\n", err)
		exit.Return(1)
	}
}

func waitForLabelKeys(labelsFilePath string, labelKeys []string) (map[string]string, error) {
	for {
		labels, err := loadDownwardAPIMap(labelsFilePath)
		if err != nil {
			return nil, err
		}

		if mapContainsKeys(labels, labelKeys) {
			return labels, nil
		}

		// TODO: Use fsnotify to reduce polling?
		time.Sleep(250 * time.Millisecond)
	}
}

func mapContainsKeys(m map[string]string, keys []string) bool {
	for _, key := range keys {
		if _, exists := m[key]; !exists {
			return false
		}
	}
	return true
}

func loadDownwardAPIMap(filePath string) (map[string]string, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("can't read file %v: %v", filePath, err)
	}
	lines := bytes.Split(data, []byte{'\n'})

	result := map[string]string{}
	for _, line := range lines {
		line = bytes.TrimSpace(line)

		if len(line) == 0 {
			continue
		}

		parts := bytes.SplitN(line, []byte{'='}, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("can't parse line: %v", line)
		}
		key := string(parts[0])
		value, err := strconv.Unquote(string(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("can't parse quoted value: %q", parts[1])
		}
		result[key] = value
	}
	return result, nil
}

func findRequiredLabelKeys(args []string) ([]string, error) {
	funcs := templateFuncs(nil)

	// Override the "label" function with a version that records which keys are
	// requested instead of looking them up.
	uniqueKeys := map[string]struct{}{}
	funcs["label"] = func(key string) string {
		uniqueKeys[key] = struct{}{}
		return ""
	}

	for _, arg := range args {
		// Parse each arg as a Go template.
		tpl, err := template.New("").Funcs(funcs).Parse(arg)
		if err != nil {
			return nil, err
		}

		// Execute the template to see what labels it asks for.
		// We don't care about the output so we just discard it.
		err = tpl.Execute(ioutil.Discard, nil)
		if err != nil {
			return nil, err
		}
	}

	// Return a sorted list of unique label keys requested.
	keys := []string{}
	for key := range uniqueKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func generateArgs(argTemplates []string, labels map[string]string) ([]string, error) {
	funcs := templateFuncs(labels)

	result := []string{}
	for _, arg := range argTemplates {
		// Parse each arg as a Go template and execute it.
		tpl, err := template.New("").Funcs(funcs).Parse(arg)
		if err != nil {
			return nil, err
		}
		buf := strings.Builder{}
		err = tpl.Execute(&buf, nil)
		if err != nil {
			return nil, err
		}
		result = append(result, buf.String())
	}
	return result, nil
}

func templateFuncs(labels map[string]string) template.FuncMap {
	// Return a new FuncMap every time so we don't need to worry about mutations.
	return template.FuncMap{
		"label": func(key string) string {
			return labels[key]
		},
		"trim": func(s, cutset string) string {
			return strings.Trim(s, cutset)
		},
	}
}
