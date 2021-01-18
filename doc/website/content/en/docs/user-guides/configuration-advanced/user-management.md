---
title: User Management and Authentication
weight: 1
aliases: ['/docs/user-guides/user-management/'] 
---

Vitess uses its own mechanism for managing users and their permissions through
 VTGate. As a result, the `CREATE USER....` and
`GRANT...` statements will not work if sent through VTGate.

## Authentication

The Vitess VTGate component takes care of authentication for requests so we
will need to add any users that should have access to the Keyspaces via the
command-line options to VTGate.

The simplest way to configure users is using a `static` auth method and we
can define the users in a JSON formatted file or string.

```sh
$ cat > users.json << EOF
{
  "vitess": [
    {
      "UserData": "vitess",
      "Password": "supersecretpassword"
    }
  ],
  "myuser1": [
    {
      "UserData": "myuser1",
      "Password": "password1"
    }
  ],
  "myuser2": [
    {
      "UserData": "myuser2",
      "Password": "password2"
    }
  ]
}
EOF
```

Then we can load this into VTGate with the additional commandline parameters:
```sh
vtgate $(cat <<END_OF_COMMAND
    -mysql_auth_server_impl=static
    -mysql_auth_server_static_file=users.json
    ...
    ...
    ...
END_OF_COMMAND
)
```

Now we can test our new users:

```
$ mysql -h 127.0.0.1 -u myuser1 -ppassword1 -e "select 1"
+---+
| 1 |
+---+
| 1 |
+---+

$ mysql -h 127.0.0.1 -u myuser1 -pincorrect_password -e "select 1"
ERROR 1045 (28000): Access denied for user 'myuser1'
```

## Password format

In the above example we used plaintext passwords.  Vitess supports the
MySQL [mysql_native_password](https://dev.mysql.com/doc/refman/8.0/en/native-pluggable-authentication.html)
hash format, and you should always specify your passwords using this
in a non-test or external environment.  Vitess does not yet support the
[caching_sha2_password](https://dev.mysql.com/doc/refman/8.0/en/caching-sha2-pluggable-authentication.html)
format that became the default for MySQL in 8.0.

To use a `mysql_native_password` hash, your user section in your static
JSON authentication file would look something like this instead:

```json
{
  "vitess": [
    {
      "UserData": "vitess",
      "MysqlNativePassword": "*9E128DA0C64A6FCCCDCFBDD0FC0A2C967C6DB36F"
    }
  ]
}
```

You can extract a `mysql_native_password` hash from an existing MySQL
install by looking at the `authentication_string` column of the relevant
user's row in the `mysql.user` table. An alternate way to generate this
hash is to SHA1 the cleartext password string twice, e.g. doing it in
MySQL for the cleartext password `password`:

```mysql
mysql> SELECT UPPER(SHA1(UNHEX(SHA1("password")))) as hash;
+------------------------------------------+
| hash                                     |
+------------------------------------------+
| 2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19 |
+------------------------------------------+
1 row in set (0.01 sec)
```

So, you would use `*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19` as the
`MysqlNativePassword` hash value for the cleartext password `password`.


## UserData

In the static authentication JSON file, the `UserData` string is **not**
the username;  the username is the string key for the list.  The `UserData`
string does **not** need to correspond to the username, and is used by the
[authorization mechanism](../authorization) when referring to a user.  It is
usually however simpler if you make the `UserData` string and the username
the same.

The `UserData` feature can be leveraged to create multiple users that are
equivalent to the authorization layer (i.e. multiple users having the same
`UserData` strings), but are different in the authentication layer (i.e.
have different usernames and passwords).

## Multiple passwords

A very convenient feature of the VTGate authorization is that, as can be
seen in the example JSON authentication files, you have a **list** of
`UserData` and `Password`/`MysqlNativePassword` pairs associated with
a user.  You can optionally leverage this to assign multiple different
passwords to a single user, and VTGate will allow a user to authenticate
with any of the defined passwords.  This makes password rotation 
much easier;  and less likely to require or cause downtime.

An example could be:
```json
{
  "vitess": [
    {
      "UserData": "vitess_old",
      "MysqlNativePassword": "*9E128DA0C64A6FCCCDCFBDD0FC0A2C967C6DB36F"
    },
    {
      "UserData": "vitess_new",
      "MysqlNativePassword": "*B3AD996B12F211BEA47A7C666CC136FB26DC96AF"
    }
  ]
}
```

This feature also allows different `UserData` strings
to be associated with a user depending on the password used.  This can
be used in concert with the [authorization mechanism](../authorization) to
migrate an application gracefully from one set of ACLs (or no ACLs)
to another set of ACLs, by just changing the password used by the
application.

In the example above, the username `vitess` has **two different** passwords
that would be allowed, each resulting in different `UserData` strings
(`vitess_old` or `vitess_new`) being passed to the VTTablet layer that can
be used for authorization/ACL enforcement.

## Other authentication methods

Other than the static authentication file method above, other authentication
mechanisms are also provided:
 * [LDAP-based authentication](../ldap_auth)
 * TLS client certificate-based authentication
