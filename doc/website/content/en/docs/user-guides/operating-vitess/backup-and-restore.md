	---
title: Backup and Restore
weight: 2
aliases: ['/docs/user-guides/backup-and-restore/']
---

Backup and Restore are integrated features provided by tablets managed by Vitess. As well as using _backups_ for data integrity, Vitess will also create and restore backups for provisioning new tablets in an existing shard.

## Concepts

Vitess supports pluggable interfaces for both [Backup Storage Services](https://github.com/vitessio/vitess/blob/master/go/vt/mysqlctl/backupstorage/interface.go) and [Backup Engines](https://github.com/vitessio/vitess/blob/master/go/vt/mysqlctl/backupengine.go).

Before backing up or restoring a tablet, you need to ensure that the tablet is aware of the Backup Storage system and Backup engine that you are using. To do so, use the following command-line flags when starting a vttablet that has access to the location where you are storing backups.

### Backup Storage Services

Currently, Vitess has plugins for:

* A network-mounted path (e.g. NFS)
* Google Cloud Storage
* Amazon S3
* Ceph

### Backup Engines

The engine is the techology used for generating the backup. Currently Vitess has plugins for:

* Builtin: Shutdown an instance and copy all the database files (default)
* XtraBackup: An online backup using Percona's XtraBackup

## VTTablet Configuration

The following options can be used to configure VTTablet for backups:

<table class="responsive">
  <thead>
    <tr>
      <th colspan="2">Flags</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>backup_storage_implementation</code></td>
      <td>Specifies the implementation of the Backup Storage interface to
        use.<br><br>
        Current plugin options available are:
        <ul>
          <li><code>file</code>: NFS or any other filesystem-mounted network
            drive.</li>
          <li><code>gcs</code>: Google Cloud Storage.</li>
          <li><code>s3</code>: Amazon S3.</li>
          <li><code>ceph</code>: Ceph Object Gateway S3 API.</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td><code>backup_engine_implementation</code></td>
      <td>Specifies the implementation of the Backup Engine to
        use.<br><br>
        Current options available are:
        <ul>
          <li><code>builtin</code>: Copy all the database files into specified storage. This is the default.</li>
          <li><code>xtrabackup</code>: Percona Xtrabackup.</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td><code>backup_storage_hook</code></td>
      <td>If set, the content of every file to backup is sent to a hook. The
        hook receives the data for each file on stdin. It should echo the
        transformed data to stdout. Anything the hook prints to stderr will
        be printed in the vttablet logs.<br>
        Hooks should be located in the <code>vthook</code> subdirectory of the
        <code>VTROOT</code> directory.<br>
        The hook receives a <code>-operation write</code> or a
        <code>-operation read</code> parameter depending on the direction
        of the data processing. For instance, <code>write</code> would be for
        encryption, and <code>read</code> would be for decryption.</br>
      </td>
    </tr>
    <tr>
      <td><code>backup_storage_compress</code></td>
      <td>This flag controls if the backups are compressed by the Vitess code.
        By default it is set to true. Use
        <code>-backup_storage_compress=false</code> to disable.</br>
        This is meant to be used with a <code>-backup_storage_hook</code>
        hook that already compresses the data, to avoid compressing the data
        twice.
      </td>
    </tr>
    <tr>
      <td><code>file_backup_storage_root</code></td>
      <td>For the <code>file</code> plugin, this identifies the root directory
        for backups.
      </td>
    </tr>
    <tr>
      <td><code>gcs_backup_storage_bucket</code></td>
      <td>For the <code>gcs</code> plugin, this identifies the
        <a href="https://cloud.google.com/storage/docs/concepts-techniques#concepts">bucket</a>
        to use.</td>
    </tr>
    <tr>
      <td><code>s3_backup_aws_region</code></td>
      <td>For the <code>s3</code> plugin, this identifies the AWS region.</td>
    </tr>
    <tr>
      <td><code>s3_backup_storage_bucket</code></td>
      <td>For the <code>s3</code> plugin, this identifies the AWS S3
        bucket.</td>
    </tr>
    <tr>
      <td><code>ceph_backup_storage_config</code></td>
      <td>For the <code>ceph</code> plugin, this identifies the path to a text
        file with a JSON object as configuration. The JSON object requires the
        following keys: <code>accessKey</code>, <code>secretKey</code>,
        <code>endPoint</code> and <code>useSSL</code>. Bucket name is computed
        from keyspace name and shard name is separated for different
        keyspaces / shards.</td>
    </tr>
    <tr>
      <td><code>restore_from_backup</code></td>
      <td>Indicates that, when started with an empty MySQL instance, the
        tablet should restore the most recent backup from the specified
        storage plugin.</td>
    </tr>
    <tr>
      <td><code>xtrabackup_root_path</code></td>
      <td>For the <code>xtrabackup</code> backup engine, directory location of the xtrabackup executable, e.g., /usr/bin</td>
    </tr>
    <tr>
      <td><code>xtrabackup_backup_flags</code></td>
      <td>For the <code>xtrabackup</code> backup engine, flags to pass to backup command. These should be space separated and will be added to the end of the command</td>
    </tr>
    <tr>
      <td><code>xbstream_restore_flags</code></td>
      <td>For the <code>xtrabackup</code> backup engine, flags to pass to xbstream command during restore. These should be space separated and will be added to the end of the command. These need to match the ones used for backup e.g. --compress / --decompress, --encrypt / --decrypt</td>
    </tr>
    <tr>
      <td><code>xtrabackup_stream_mode</code></td>
      <td>For the <code>xtrabackup</code> backup engine, which mode to use if streaming, valid values are <code>tar</code> and <code>xbstream</code>. Defaults to <code>tar</code></td>
    </tr>
    <tr>
      <td><code>xtrabackup_user</code></td>
      <td>For the <code>xtrabackup</code> backup engine, required user that xtrabackup will use to connect to the database server. This user must have all necessary privileges. For details, please refer to xtrabackup documentation.</td>
    </tr>
    <tr>
      <td><code>xtrabackup_stripes</code></td>
      <td>For the <code>xtrabackup</code> backup engine, if greater than 0, use data striping across this many destination files to parallelize data transfer and decompression</td>
    </tr>
    <tr>
      <td><code>xtrabackup_stripe_block_size</code></td>
      <td>For the <code>xtrabackup</code> backup engine, size in bytes of each block that gets sent to a given stripe before rotating to the next stripe</td>
    </tr>
  </tbody>
</table>

### Authentication

Note that for the Google Cloud Storage plugin, we currently only support
[Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials).
It means that access to Cloud Storage is automatically granted by virtue of
the fact that you're already running within Google Compute Engine or Container
Engine.

For this to work, the GCE instances must have been created with the [scope](https://cloud.google.com/compute/docs/authentication#using) that grants read-write access to Cloud Storage. When using Container Engine, you can
do this for all the instances it creates by adding `--scopes storage-rw` to the `gcloud container clusters create` command.

### Backup Frequency

We recommend to take backups regularly e.g. you should set up a cron job for it.

To determine the proper frequency for creating backups, consider the amount of time that you keep replication logs and allow enough time to investigate and fix problems in the event that a backup operation fails.

For example, suppose you typically keep four days of replication logs and you create daily backups. In that case, even if a backup fails, you have at least a couple of days from the time of the failure to investigate and fix the problem.

### Concurrency

The backup and restore processes simultaneously copy and either compress or decompress multiple files to increase throughput. You can control the concurrency using command-line flags:

* The vtctl [Backup](https://vitess.io/docs/reference/programs/vtctl/tablets/#backup) command uses the `-concurrency` flag.
* vttablet uses the `-restore_concurrency` flag.

If the network link is fast enough, the concurrency matches the CPU usage of the process during the backup or restore process.

## Creating a backup

Run the following vtctl command to create a backup:

``` sh
vtctl Backup <tablet-alias>
```

If the engine is `builtin`, in response to this command, the designated tablet performs the following
sequence of actions:

1. Switches its type to `BACKUP`. After this step, the tablet is no
   longer used by VTGate to serve any query.

1. Stops replication, gets the current replication position (to be saved in the
   backup along with the data).

1. Shuts down its mysqld process.

1. Copies the necessary files to the Backup Storage implementation that was
   specified when the tablet was started. Note if this fails, we still keep
   going, so the tablet is not left in an unstable state because of a storage
   failure.

1. Restarts mysqld.

1. Restarts replication (with the right semi-sync flags corresponding to its
   original type, if applicable).

1. Switches its type back to its original type. After this, it will most likely
   be behind on replication, and not used by VTGate for serving until it catches
   up.

If the engine is `xtrabackup`, we do not do any of the above. The tablet can
continue to serve traffic while the backup is running.

## Restoring a backup

When a tablet starts, Vitess checks the value of the
`-restore_from_backup` command-line flag to determine whether
to restore a backup to that tablet.

* If the flag is present, Vitess tries to restore the most recent backup from
  the Backup Storage system when starting the tablet.
* If the flag is absent, Vitess does not try to restore a backup to the
  tablet. This is the equivalent of starting a new tablet in a new shard.

As noted in the [Configuration](#vttablet-configuration) section, the flag is
generally enabled all of the time for all of the tablets in a shard.
By default, if Vitess cannot find a backup in the Backup Storage system,
the tablet will start up empty. This behavior allows you to bootstrap a new
shard before any backups exist.

If the `-wait_for_backup_interval` flag is set to a value greater than zero,
the tablet will instead keep checking for a backup to appear at that interval.
This can be used to ensure tablets launched concurrently while an initial backup
is being seeded for the shard (e.g. uploaded from cold storage or created by
another tablet) will wait until the proper time and then pull the new backup
when it's ready.

``` sh
vttablet ... -backup_storage_implementation=file \
             -file_backup_storage_root=/nfs/XXX \
             -restore_from_backup
```

## Managing backups

**vtctl** provides two commands for managing backups:

* [ListBackups](https://vitess.io/docs/reference/programs/vtctl/shards/#listbackups) displays the
    existing backups for a keyspace/shard in chronological order.

    ``` sh
    vtctl ListBackups <keyspace/shard>
    ```

* [RemoveBackup](https://vitess.io/docs/reference/programs/vtctl/shards/#removebackup) deletes a
    specified backup for a keyspace/shard.

    ``` sh
    RemoveBackup <keyspace/shard> <backup name>
    ```

## Bootstrapping a new tablet

Bootstrapping a new tablet is almost identical to restoring an existing tablet.
The only thing you need to be cautious about is that the tablet specifies its
keyspace, shard and tablet type when it registers itself at the topology.
Specifically, make sure that the following additional vttablet parameters are set:

``` 
    -init_keyspace <keyspace>
    -init_shard <shard>
    -init_tablet_type replica|rdonly
```

The bootstrapped tablet will restore the data from the backup and then apply
changes, which occurred after the backup, by restarting replication.

## Backing up Topology Server

The Topology Server stores metadata (and not tablet data). It is recommended to create a backup using the method described by the underlying plugin:

* [etcd](https://etcd.io/docs/v3.4.0/op-guide/recovery/)
* [ZooKeeper](http://zookeeper.apache.org/doc/r3.6.0/zookeeperAdmin.html#sc_dataFileManagement)
* [Consul](https://www.consul.io/docs/commands/snapshot.html)
