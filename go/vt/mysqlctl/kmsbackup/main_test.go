package kmsbackup

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/stretchr/testify/require"

	"github.com/planetscale/common-libs/files"
)

var (
	awsCredFile      = flag.String("aws-credentials-file", "", "AWS Credentials file")
	awsCredProfile   = flag.String("aws-credentials-profile", "", "Profile for AWS Credentials")
	awsKMSKeyARN     = flag.String("aws-kms-key-arn", "", "AWS KMS Key ARN")
	awsRegion        = flag.String("aws-region", "us-east-1", "AWS Region")
	awsS3Bucket      = flag.String("aws-s3-bucket", "planetscale-vitess-private-ci", "Bucket to use for S3 for AWS Credentials")
	awsS3SSEKMSKeyID = flag.String("aws-s3-sse-kms-key-id", "arn:aws:kms:us-east-1:997601596833:key/c25827d2-1c7f-48ad-9c62-b4cb94c60277", "KMS Key ID to use for S3-SSE")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

// createTempFile creates a temp file with the provided contents
// and returns the name of the file.
func createTempFile(t *testing.T, content string) string {
	t.Helper()
	tmpfile, err := os.CreateTemp("", "backup_labels_test")
	require.NoError(t, err)
	defer tmpfile.Close()

	_, err = tmpfile.Write([]byte(content))
	require.NoError(t, err)
	return tmpfile.Name()
}

func newAWSConfig() aws.Config {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithSharedCredentialsFiles([]string{*awsCredFile}),
		config.WithSharedConfigProfile(*awsCredProfile),
		config.WithRegion(*awsRegion),
	)
	if err != nil {
		panic(err)
	}
	return cfg
}

func newEncryptedS3FilesBackupStorage(cfg aws.Config) (*FilesBackupStorage, error) {
	ufs := files.NewS3Files(cfg, *awsRegion, *awsS3Bucket, *awsS3SSEKMSKeyID, "")

	fs, err := files.NewEncryptedS3Files(cfg, *awsRegion, *awsS3Bucket, *awsS3SSEKMSKeyID, "", *awsKMSKeyARN)
	if err != nil {
		return nil, err
	}

	return &FilesBackupStorage{
		arn:              "not-used",
		region:           *awsRegion,
		bucket:           *awsS3Bucket,
		kmsKeyID:         *awsS3SSEKMSKeyID,
		files:            fs,
		unencryptedFiles: ufs,
	}, nil
}

func newS3FilesBackupStorage(cfg aws.Config) (*FilesBackupStorage, error) {
	fs := files.NewS3Files(cfg, *awsRegion, *awsS3Bucket, *awsS3SSEKMSKeyID, "")

	return &FilesBackupStorage{
		arn:              "not-used",
		region:           *awsRegion,
		bucket:           *awsS3Bucket,
		kmsKeyID:         *awsS3SSEKMSKeyID,
		files:            fs,
		unencryptedFiles: fs,
	}, nil
}

func newLocalFilesBackupStorage(testDir string) (*FilesBackupStorage, error) {
	fs, err := files.NewLocalFiles(testDir)

	if err != nil {
		return nil, err
	}

	return &FilesBackupStorage{
		arn:              "not-used",
		region:           *awsRegion,
		bucket:           *awsS3Bucket,
		kmsKeyID:         *awsS3SSEKMSKeyID,
		files:            fs,
		unencryptedFiles: fs,
	}, nil
}

func testFilesBackupStorage(t *testing.T) *FilesBackupStorage {
	t.Helper()

	if *awsCredFile == "" {
		return testLocalFilesBackupStorage(t)
	} else if *awsKMSKeyARN == "" {
		return testS3FilesBackupStorage(t, newAWSConfig())
	}
	return testEncryptedS3FilesBackupStorage(t, newAWSConfig())
}

func testEncryptedS3FilesBackupStorage(t *testing.T, cfg aws.Config) *FilesBackupStorage {
	t.Helper()

	fbs, err := newEncryptedS3FilesBackupStorage(cfg)
	require.NoError(t, err)
	return fbs
}

func testLocalFilesBackupStorage(t *testing.T) *FilesBackupStorage {
	t.Helper()

	fbs, err := newLocalFilesBackupStorage(t.TempDir())
	require.NoError(t, err)
	return fbs
}

func testS3FilesBackupStorage(t *testing.T, cfg aws.Config) *FilesBackupStorage {
	t.Helper()

	fbs, err := newS3FilesBackupStorage(cfg)
	require.NoError(t, err)
	return fbs
}
