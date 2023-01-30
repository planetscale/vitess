package kmsbackup

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"

	"github.com/stretchr/testify/require"
)

// TestEncryptedStorage_ReadConnResetError tests that how
// kmsbackup.FilesBackupStorage responds to "read: connection reset" errors
// encountered during KMS GenerateDataKey operations.
func TestEncryptedStorage_ReadConnResetError(t *testing.T) {
	if *awsCredFile == "" {
		t.Skip("--aws-credentials-file is not set")
	}

	if *awsKMSKeyARN == "" {
		t.Skip("--aws-kms-key-arn is not set")
	}

	// The AWS request.DefaultRetryer does not retry "read: connection reset" errors.
	testEncryptedStorageReadConnResetError(t, newAWSConfig(), 0)

	// Verify that our custom retryer does retry those errors.
	retryer := newKMSReadConnResetRetryer()
	expectRetries := retryer.DefaultRetryer.NumMaxRetries
	testEncryptedStorageReadConnResetError(t, newAWSConfigWithRetryer(retryer), expectRetries)
}

func testEncryptedStorageReadConnResetError(t *testing.T, cfg *aws.Config, expectRetries int) {
	ctx := context.Background()

	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	t.Setenv(annotationsFilePath, tmpfile)

	sess, err := session.NewSession(cfg)
	require.NoError(t, err)

	// Force a "read: connection reset" error for KMS GenerateDataKey operations.
	readConnResetErr, err := simulateReadConnectionResetError()
	require.NoError(t, err)
	require.ErrorContains(t, readConnResetErr, "read: connection reset")
	sess.Handlers.Send.PushBack(func(r *request.Request) {
		if r.ClientInfo.ServiceID == kms.ServiceID && r.Operation.Name == "GenerateDataKey" {
			r.Error = awserr.New("Unknown", "send request failed", readConnResetErr)
		}
	})

	// Count retries.
	var retries int
	sess.Handlers.Complete.PushBack(func(r *request.Request) {
		if r.ClientInfo.ServiceID == kms.ServiceID && r.Operation.Name == "GenerateDataKey" {
			retries = r.RetryCount
		}
	})

	fbs := testEncryptedS3FilesBackupStorage(t, sess)

	handle, err := fbs.StartBackup(ctx, "a", "b")
	require.NoError(t, err)

	_, err = handle.AddFile(ctx, "ssfile", 10)
	require.Error(t, err)
	require.Equal(t, expectRetries, retries)
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
