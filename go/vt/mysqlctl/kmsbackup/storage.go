package kmsbackup

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/planetscale/common-libs/files"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	annotationsFilePath              = "ANNOTATIONS_FILE_PATH"
	lastBackupLabel                  = "psdb.co/last-backup-id"
	lastBackupExcludedKeyspacesLabel = "psdb.co/last-backup-excluded-keyspaces"
	backupIDLabel                    = "psdb.co/backup-id"
)

var (
	backupRegion      = flag.String("psdb.backup_region", "", "The region for the backups")
	backupBucket      = flag.String("psdb.backup_bucket", "", "S3 bucket for backups")
	backupARN         = flag.String("psdb.backup_arn", "", "ARN for the S3 bucket")
	backupSSEKMSKeyID = flag.String("psdb.backup_sse_kms_key_id", "", "KMS Key ID to use for S3-SSE")
)

func init() {
	backupstorage.BackupStorageMap["kmsbackup"] = &FilesBackupStorage{}
}

// FilesBackupStorage satisfies backupstorage.BackupStorage.
type FilesBackupStorage struct {
	// region specified the region for the backup
	region string

	// bucket defines the S3 bucket for backups.
	bucket string

	// arn represents the ARN for the encrypted S3 bucket.
	arn string

	// kmsKeyID is the KMS Key ID for S3-SSE
	kmsKeyID string

	// files represents the a file system abstraction. It can be
	// replaced for testing purposes.
	files files.Files

	// unencryptedFiles is a file system that doesn't encrypt the
	// data. It is currencly used for the SIZE file. It can be
	// replaced for testing purposes.
	unencryptedFiles files.Files
}

// ListBackups satisfies backupstorage.BackupStorage.
// This is a custom implementation that returns at most a single value based on the pod label.
// It uses the k8s downward api feature to extract the label values.
func (f *FilesBackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	lastBackupID, err := loadTag(lastBackupLabel)
	if err != nil {
		return nil, err
	}
	// lastBackupID won't be set if there was no previous backup.
	if lastBackupID == "" {
		return nil, nil
	}

	excludedKeyspaces, err := loadTag(lastBackupExcludedKeyspacesLabel)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(dir, "/")
	if len(parts) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid backup directory: %v", dir)
	}

	keyspace := parts[0]
	for _, excludedKeyspace := range strings.Split(excludedKeyspaces, ",") {
		if keyspace == excludedKeyspace {
			// We don't have this keyspace in the backup, so skip it.
			return nil, nil
		}
	}

	// We have to provide a vitess compliant name. Some vitess tools parse this info.
	// This code is copied from mysqlctl/backup.go.
	tabletAlias := &topodatapb.TabletAlias{
		Cell: "vtbackup",
		Uid:  1,
	}
	name := fmt.Sprintf("%v.%v", time.Now().UTC().Format(mysqlctl.BackupTimestampFormat), tabletAlias)

	fbh, err := f.createHandle(ctx, lastBackupID, dir, name)
	if err != nil {
		return nil, err
	}

	return []backupstorage.BackupHandle{fbh}, nil
}

// StartBackup satisfies backupstorage.BackupStorage.
func (f *FilesBackupStorage) StartBackup(ctx context.Context, dir string, name string) (backupstorage.BackupHandle, error) {
	backupID, err := loadTag(backupIDLabel)
	if err != nil {
		return nil, err
	}

	handle, err := f.createHandle(ctx, backupID, dir, name)
	if err != nil {
		return nil, err
	}

	if err := handle.createRoot(ctx); err != nil {
		return nil, err
	}

	log.Infof("Starting backup with id = %s, bucket = %s, kmsKeyID = %s, root = %s, name = %s.",
		backupID, f.bucket, f.kmsKeyID, dir, name)

	return handle, nil
}

func (f *FilesBackupStorage) createHandle(ctx context.Context, backupID, dir, name string) (*filesBackupHandle, error) {
	if backupID == "" {
		return nil, errors.New("backup_id is not specified")
	}

	// flags are parsed later, hence we need to assign them here
	if *backupRegion != "" {
		f.region = *backupRegion
	}
	if *backupBucket != "" {
		f.bucket = *backupBucket
	}
	if *backupARN != "" {
		f.arn = *backupARN
	}
	if *backupSSEKMSKeyID != "" {
		f.kmsKeyID = *backupSSEKMSKeyID
	}

	if f.region == "" {
		return nil, errors.New("backup_region is not specified")
	}

	if f.bucket == "" {
		return nil, errors.New("backup_bucket is not specified")
	}

	if f.arn == "" {
		return nil, errors.New("backup_arn is not specified")
	}

	if f.kmsKeyID == "" {
		return nil, errors.New("backup_sse_kms_key_id is not specified")
	}

	rootPath := path.Join("/", backupID, dir)

	cfg := request.WithRetryer(aws.NewConfig(), newKMSReadConnResetRetryer())

	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to initialize aws session")
	}

	impl := f.files
	if impl == nil {
		impl, err = files.NewEncryptedS3Files(sess, f.region, f.bucket, f.kmsKeyID, "", f.arn)
		if err != nil {
			return nil, vterrors.Wrap(err, "could not create encrypted s3 files")
		}
	}

	unencryptedFs := f.unencryptedFiles
	if unencryptedFs == nil {
		unencryptedFs = files.NewS3Files(sess, f.region, f.bucket, f.kmsKeyID, "")
	}

	return newFilesBackupHandle(impl, unencryptedFs, rootPath, dir, name), nil
}

// RemoveBackup satisfies backupstorage.BackupStorage.
// This function is a no-op because removal of backups is handled by singularity.
func (f *FilesBackupStorage) RemoveBackup(ctx context.Context, dir string, name string) error {
	return nil
}

// Close satisfies backupstorage.BackupStorage.
// This function is a no-op because an aws session does not need to be closed.
func (f *FilesBackupStorage) Close() error {
	return nil
}

// WithParams satisfies backupstorage.BackupStorage.
// This function is currently a no-op.
// It may be used to return a new *FilesBackupStorage with the provided
// backupstorage.Params.
func (f *FilesBackupStorage) WithParams(backupstorage.Params) backupstorage.BackupStorage {
	// TODO(maxeng): return a new *FilesBackupStorage that uses params.
	return f
}

func loadTag(label string) (string, error) {
	tags, err := loadTags()
	if err != nil {
		return "", err
	}
	return tags[label], nil
}

// loadTags was adapted from vttablet-starter/main.go.
func loadTags() (map[string]string, error) {
	result := map[string]string{}

	filePath := os.Getenv(annotationsFilePath)
	if filePath == "" {
		return nil, fmt.Errorf("%v was not specified", annotationsFilePath)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("can't read file %v: %v", filePath, err)
	}
	lines := bytes.Split(data, []byte{'\n'})

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