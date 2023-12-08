package kmsbackup

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/stretchr/testify/require"
)

// TestEncryptedStorage_ReadConnResetError tests that how
// kmsbackup.FilesBackupStorage responds to "read: connection reset" errors
// encountered during KMS GenerateDataKey operations.
func TestEncryptedStorage_ReadConnResetError(t *testing.T) {
	// Verify that the built-in retryer retries those errors.
	expectRetries := retry.NewStandard().MaxAttempts()
	testEncryptedStorageReadConnResetError(t, newAWSConfig(), expectRetries)
}

func testEncryptedStorageReadConnResetError(t *testing.T, cfg aws.Config, expectRetries int) {
	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	t.Setenv(annotationsFilePath, tmpfile)

	// Force a "read: connection reset" error for KMS GenerateDataKey operations.
	readConnResetErr, err := simulateReadConnectionResetError()
	require.NoError(t, err)
	require.ErrorContains(t, readConnResetErr, "read: connection reset")

	retryer := retry.NewStandard()
	require.True(t, retryer.IsErrorRetryable(readConnResetErr))

	delay, err := retryer.RetryDelay(1, readConnResetErr)
	require.NoError(t, err)
	require.Greater(t, delay, 0*time.Second)
}

// simulateReadConnectionResetError creates a "read: connection reset" error.
//
// See: https://gosamples.dev/connection-reset-by-peer/
func simulateReadConnectionResetError() (error, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	defer listener.Close()

	doneCh := make(chan error, 1)

	accept := func(listener net.Listener, doneCh chan error) {
		conn, err := listener.Accept()

		if err != nil {
			doneCh <- err
			return
		}

		b := make([]byte, 1)
		if _, err := conn.Read(b); err != nil {
			doneCh <- err
			return
		}

		if err := conn.Close(); err != nil {
			doneCh <- err
			return
		}

		close(doneCh)
	}

	go accept(listener, doneCh)

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		return nil, nil
	}
	defer conn.Close()
	b := []byte("hi")

	if _, err := conn.Write(b); err != nil {
		return nil, err
	}

	if err := <-doneCh; err != nil {
		return nil, err
	}

	_, err = conn.Read(b)

	return err, nil
}
