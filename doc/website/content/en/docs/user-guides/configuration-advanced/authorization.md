---
title: Authorization
weight: 3
aliases: ['/docs/user-guides/authorization/'] 
---

A common question is how to enforce fine-grained access control in Vitess.
This question comes up because Vitess uses connection pooling with fixed
MySQL users at the VTTablet level, and implements its own authentication
at the VTGate level. As a result, you cannot use the normal MySQL GRANTs
system to give certain application-level MySQL users more or less permissions
than others.

The MySQL GRANT system is very extensive, and we have not reimplemented
all of this functionality in Vitess.  What we have done is to enable you
to provide authorization via table-level ACLs, with a few basic
characteristics:

 * Individual users can be assigned 3 levels of permissions:
   * Read (corresponding to read DML, e.g. `SELECT`)
   * Write (corresponding to write DML, e.g. `INSERT`, `UPDATE`, `DELETE`)
   * Admin (corresponding to DDL, e.g. `ALTER TABLE`)
 * Permissions are applied on a specified set of tables, which can be
   enumerated or specified by regex.

## VTTablet parameters for table ACLs

Note that the Vitess authorization via ACLs are applied at the VTTablet
level, as opposed to on VTGate, where authentication is enforced.
There are a number of VTTablet command line parameters that control the
behavior of ACLs.  Let's review these:

 * `-enforce-tableacl-config`:  Set this to `true` to ensure VTTablet will not
   start unless there is a valid ACL configuration. This is used to
   catch misconfigurations resulting in blanket access to authenticated
   users.
 * `-queryserver-config-enable-table-acl-dry-run`:  Set to `true` to check the
   table ACL at runtime, and only emit the
   [TableACLPseudoDenied](../../configuration-basic/configuring-components/#monitoring)
   metric if a request would have been blocked. The request is then
   allowed to pass, even if the ACL determined it should
   be blocked.  This can be used for testing new or updated ACL policies.
   Default is `false`.
 * `-queryserver-config-strict-table-acl`: Set to `true` to enforce table ACL
   checking.  **This needs to be enabled for your ACLs to have any effect.**
   Any users that are not specified in an ACL policy will be denied.
   Default is `false`.
 * `-queryserver-config-acl-exempt-acl`:  Allows you to specify the name
   of an ACL (see below for format) that is exempt from enforcement.
   Allows you to separate the rollout and the subsequent enforcement of
   a specific ACL.
 * `-table-acl-config`: Path to a file defining the table ACL config.
 * `-table-acl-config-reload-interval`:  How often the `table-acl-config`
   should be reloaded.  Set this to allow you to update the ACL file on
   disk, and then have VTTablet automatically reload the file within this
   period.  Default is not to reload the ACL file after VTTablet startup.
   Note that even if you do not set this parameter, you can always force
   VTTablet to reload the ACL config file from disk by sending a SIGHUP
   signal to your VTTablet process.

## Format of the table ACL config file

The file specified in the `-table-acl-config` parameter above is a JSON
file with the following example to explain the format:

```json
{
    "table_groups": [
        {
            "name": "aclname",
            "table_names_or_prefixes": [
                "%"
            ],
            "readers": [
                "vtgate-user1"
            ],
            "writers": [
                "vtgate-user2"
            ],
            "admins": [
                "vtgate-user3"
            ]
        },
        { "... more ACLs here if necessary ..." }
    ]
}
```

Notes:

 * `name`: This is the name of the ACL (`aclname` in the example above) and is
   what needs to be specified in `-queryserver-config-acl-exempt-acl`,
   if you need to exempt a specific ACL from enforcement.
 * `table_names_or_prefixes`:  A list of strings and/or regexes that allow
   a rule to target a specific table or set of tables.  Use `%` as in the
   example to specify all tables.  Note that only the SQL `%` "regex"
   wildcard is supported here at the moment.
 * `readers`:  A list of VTGate users, specified by their [UserData](../../configuration-advanced/user-management/#userdata)
   field in the authentication specification, that are allowed to read the
   tables targeted by this ACL rule. Typically allows `SELECT`.
 * `writers`:  A list of VTGate users that are allowed to write to the tables
   targeted by this ACL rule. Typically allows `INSERT`, `UPDATE` and `DELETE`.
 * `admins`:  A list of VTGate users that are allowed admin privileges on
   the tables targeted by this ACL rule.  Typically allows DDL privileges,
   e.g. `ALTER TABLE`. Note that this also includes some commands that might
   be thought of as DML, which are really DDL, like `TRUNCATE`)
 * Note that `writers` privilege does not imply `readers` privilege, and `admins`
   privilege does not imply `readers` or `writers`.  You need to therefore
   add your users to each list explicitly if you want them to have that
   level of access.
 * You cannot use multiple ACL rules to target the same (sub)set of tables.
   Therefore the tablenames specified by `table_names_or_prefixes`
   (or expanded by regexes) need to be non-overlapping between ACL rules.
   Additionally, you cannot have duplicate tablenames or overlapping regexes
   in the `table_names_or_prefixes` list in a single ACL rule.

## Example

Let's assume your Vitess cluster already has two keyspaces setup:

 * `keyspace1` with a single table `t` that should only be accessed by `myuser1`
 * `keyspace2` with a single table `t` that should only be accessed by `myuser2`

For the VTTablet configuration for `keyspace1`:
```sh
$ cat > acls_for_keyspace1.json << EOF
{
  "table_groups": [
    {
      "name": "keyspace1acls",
      "table_names_or_prefixes": ["%"],
      "readers": ["myuser1", "vitess"],
      "writers": ["myuser1", "vitess"],
      "admins": ["myuser1", "vitess"]
    }
  ]
}
EOF

$ vttablet -init_keyspace "keyspace1" -table-acl-config=acls_for_keyspace1.json -enforce-tableacl-config -queryserver-config-strict-table-acl ........
```

Note that the `%` specifier for `table_names_or_prefixes` translates to
"all tables".

Do the same thing for `keyspace2`:
```sh
$ cat > acls_for_keyspace2.json << EOF
{
  "table_groups": [
    {
      "name": "keyspace2acls",
      "table_names_or_prefixes": ["%"],
      "readers": ["myuser2", "vitess"],
      "writers": ["myuser2", "vitess"],
      "admins": ["myuser2", "vitess"]
    }
  ]
}
EOF

$ vttablet -init_keyspace "keyspace2" -table-acl-config=acls_for_keyspace2.json -enforce-tableacl-config -queryserver-config-strict-table-acl ........
```

With this setup, the `myuser1` and `myuser2` users can only access their respective keyspaces, but the `vitess`
user can access both.

```sh
# Attempt to access keyspace1 with myuser2 credentials through vtgate
$ mysql -h 127.0.0.1 -u myuser2 -ppassword2 -D keyspace1 -e "select * from t"
ERROR 1045 (HY000) at line 1: vtgate: http://vtgate-zone1-7fbfd8cc47-tchbz:15001/: target: keyspace1.-80.master, used tablet: zone1-476565201 (zone1-keyspace1-x-80-replica-1.vttablet): vttablet: rpc error: code = PermissionDenied desc = table acl error: "myuser2" [] cannot run PASS_SELECT on table "t" (CallerID: myuser2)
target: keyspace1.80-.master, used tablet: zone1-1289569200 (zone1-keyspace1-80-x-replica-0.vttablet): vttablet: rpc error: code = PermissionDenied desc = table acl error: "myuser2" [] cannot run PASS_SELECT on table "t" (CallerID: myuser2)
$
```

Whereas myuser1 is able to access its keyspace without error:
```sh
$ mysql -h 127.0.0.1 -u myuser1 -ppassword1 -D keyspace1 -e "select * from t"
$
```
