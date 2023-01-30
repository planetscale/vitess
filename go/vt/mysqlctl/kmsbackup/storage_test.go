package kmsbackup

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileBackupStorage_ListBackups(t *testing.T) {
	ctx := context.Background()

	content := fmt.Sprintf(`%s="1234"`, lastBackupLabel)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	t.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)

	backups, err := fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 1, len(backups))

	backup := backups[0]
	assert.Equal(t, "a/b", backup.Directory())
	assert.NotEmpty(t, backup.Name())

	fbh := backup.(*filesBackupHandle)
	assert.Equal(t, "/1234/a/b", fbh.rootPath)

	// It should fail if backup params are not set.
	fbs.region = ""
	_, err = fbs.ListBackups(ctx, "a/b")
	assert.Error(t, err)

	// ListBackups should return empty if the label is not set.
	tmpfile = createTempFile(t, "")
	t.Cleanup(func() { os.Remove(tmpfile) })

	t.Setenv(annotationsFilePath, tmpfile)

	backups, err = fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 0, len(backups))

	// it should fail if loadDownwardAPIMap fails.
	t.Setenv(annotationsFilePath, "nosuchfile")
	_, err = fbs.ListBackups(ctx, "a/b")
	assert.Error(t, err)
}

func TestFilesBackupStorage_ListBackups_withExcludedKeyspace(t *testing.T) {
	ctx := context.Background()

	content := fmt.Sprintf("%s=\"1234\"\n%s=\"not-included\"", lastBackupLabel, lastBackupExcludedKeyspacesLabel)
	tmpfile := createTempFile(t, content)
	defer os.Remove(tmpfile)
	t.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)
	backups, err := fbs.ListBackups(ctx, "a/b")
	assert.NoError(t, err)
	require.Equal(t, 1, len(backups))

	backups, err = fbs.ListBackups(ctx, "not-included/b")
	assert.NoError(t, err)
	require.Equal(t, 0, len(backups))
}

func TestFilesBackupStorage_StartBackup(t *testing.T) {
	ctx := context.Background()

	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	t.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)

	handle, err := fbs.StartBackup(ctx, "a", "b")
	require.NoError(t, err)

	w, err := handle.AddFile(ctx, "ssfile", 10)
	require.NoError(t, err)

	input := []byte("test content")
	_, err = w.Write(input)
	require.NoError(t, err)
	w.Close()
}

func TestFilesBackupStorage_StartBackup_Complete(t *testing.T) {
	ctx := context.Background()

	// annotations config.
	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })
	t.Setenv(annotationsFilePath, tmpfile)

	// init the storage.
	fbs := testFilesBackupStorage(t)

	// create a backup.
	h, err := fbs.StartBackup(ctx, "dir", "b")
	require.NoError(t, err)

	// add a dummy manifest to indicate that this backup is complete.
	f, err := h.AddFile(ctx, backupManifestFileName, 3)
	require.NoError(t, err)

	_, err = f.Write([]byte("abc"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, h.EndBackup(ctx))

	// we expect an error because that backup is complete.
	_, err = fbs.StartBackup(ctx, "dir", "b")
	require.Error(t, err)
	require.EqualError(t, err, `kmsbackup: cannot start backup because the manifest already exists`)
}

func TestFilesBackupStorage_StartBackup_sanityCheck(t *testing.T) {
	ctx := context.Background()

	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	t.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)

	handle, err := fbs.StartBackup(ctx, "a", "b")
	require.NoError(t, err)
	t.Cleanup(func() {
		handle.(*filesBackupHandle).fs.RemoveAll(ctx, fmt.Sprintf("/%v", backupID))
	})

	w, err := handle.AddFile(ctx, "ssfile", 10)
	require.NoError(t, err)
	w.Write([]byte("test content"))
	require.NoError(t, err)
	w.Close()

	w, err = handle.AddFile(ctx, backupManifestFileName, 10)
	require.NoError(t, err)
	w.Close()

	// make sure that a nonexistent file causes an error
	fbh := handle.(*filesBackupHandle)
	fbh.filesAdded["/nonexistent"] = int64(10)
	_, err = handle.AddFile(ctx, backupManifestFileName, 10)
	assert.Error(t, err)

}

func TestFilesBackupStorage_API(t *testing.T) {
	ctx := context.Background()
	fbs := testFilesBackupStorage(t)
	assert.NoError(t, fbs.RemoveBackup(ctx, "", ""))
	assert.NoError(t, fbs.Close())
}

func TestFilesBackupStorage_StartBackup_uploadSizeFile(t *testing.T) {
	ctx := context.Background()

	backupID := rand.Int63()
	content := fmt.Sprintf(`%v="%v"`, backupIDLabel, backupID)
	tmpfile := createTempFile(t, content)
	t.Cleanup(func() { os.Remove(tmpfile) })

	t.Setenv(annotationsFilePath, tmpfile)

	fbs := testFilesBackupStorage(t)

	handle, err := fbs.StartBackup(ctx, "a", "b")
	require.NoError(t, err)
	t.Cleanup(func() {
		handle.(*filesBackupHandle).fs.RemoveAll(ctx, fmt.Sprintf("/%v", backupID))
	})

	w, err := handle.AddFile(ctx, "ssfile", 10)
	require.NoError(t, err)
	_, err = w.Write([]byte("test content"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	w, err = handle.AddFile(ctx, backupManifestFileName, 10)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	input := []byte("20")
	r, err := handle.ReadFile(ctx, "SIZE")
	require.NoError(t, err)
	output, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, input, output)
	r.Close()
}
