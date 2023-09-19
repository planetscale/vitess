// Package gcsbackup implements Vitess backup plugin over GCS.
//
// The package uses the Storage SDK and Tink SDK to perform client side
// envelope encryption for the backup while storing it in GCS.
package gcsbackup

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/servenv"
)

// The only exported API we expose to Vitess is kmsbackup.Plugin. Ensure we
// properly implement its backup interfaces. All other logic lives in this
// repository.
var (
	_ backupstorage.BackupHandle  = (*handle)(nil)
	_ backupstorage.BackupStorage = (*Storage)(nil)

	s = &Storage{}
)

func registerPlugin(fs *pflag.FlagSet) {
	fs.StringVar(&s.Bucket, "psdb.gcs_backup.bucket", "", "GCS bucket to use for backups.")
	fs.StringVar(&s.CredsPath, "psdb.gcs_backup.creds_path", "", "Credentials JSON for service account to use.")
	fs.StringVar(&s.KeyURI, "psdb.gcs_backup.key_uri", "", "GCP KMS Keyring URI to use.")

	backupstorage.BackupStorageMap["gcsbackup"] = s
}

// Init registers the backup plugin.
func init() {
	servenv.OnParseFor("vtbackup", registerPlugin)
	servenv.OnParseFor("vtctl", registerPlugin)
	servenv.OnParseFor("vtctld", registerPlugin)
	servenv.OnParseFor("vttablet", registerPlugin)
}
