/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtctl

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/wrangler"
)

var (
	defaultMaxBufferSize = 1000
	dataRoot             = os.Getenv("VTDATAROOT")
	root                 = os.Getenv("VTROOT")
	cell                 = "test"

	// topoImplementation is the flag for which implementation to use.
	// topoImplementation = flag.String("topo_implementation", "", "the topology implementation to use")

	// // topoGlobalServerAddress is the address of the global topology
	// // server.
	// topoGlobalServerAddress = flag.String("topo_global_server_address", "", "the address of the global topology server")

	// // topoGlobalRoot is the root path to use for the global topology
	// // server.
	// topoGlobalRoot = flag.String("topo_global_root", "", "the path of the global topology data in the global topology server")
)

func init() {
	addCommand("Migrasion", command{
		"SqlImport",
		importSQL,
		"[-file=<file path>]",
		"Import from mysqldump."})
}

func importSQL(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	filename := subFlags.String("file", "", "import file path")
	database := subFlags.String("database", "", "import name of database to insert data")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	_ = database
	if *filename == "" {
		return fmt.Errorf("file name is missing %v", args)
	}

	// f, err := os.Open(*filename)
	// if err != nil {
	// 	return err
	// }

	os.Stdin.SetWriteDeadline(time.Now())

	reader := bufio.NewScanner(os.Stdin)

	var setVarCmds string

	for reader.Scan() {
		line := strings.TrimSpace(reader.Text())
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		if strings.HasPrefix(line, "CREATE DATABASE") {

		}

		setVarCmds += line + "\n"
	}

	// create vitess setup (mysql and vttablet) with dbname in createDB string
	// dbName := GetDBNameFromCreateDBString(createDB)
	// err := CreateDatabase(dbName)

	return nil
}

// // GetDBNameFromCreateDBString returns database name from create database string.
// func GetDBNameFromCreateDBString(createDB string) string {
// 	s := strings.Split(createDB, "`")
// 	return s[1]
// }

// // GetTabletUID generates random UID.
// func GetTabletUID() int {
// 	return rand.Intn(100)
// }

// // CreateDatabase creates vttablet and mysql with the given database name.
// func CreateDatabase(dbName string) error {

// 	uid := GetTabletUID()

// 	// creating mysql instance
// 	err := os.Mkdir(dataRoot+"/backups", os.ModeDir)
// 	if err != nil && !os.IsExist(err) {
// 		return err
// 	}

// 	return nil
// }

// // SetupMySQL starts mysql corresponds to dbname and uid.
// func SetupMySQL(dbName string, uid int) error {
// 	mysqlPort := 17000 + uid
// 	fmt.Println("Starting MySQL")
// 	proc := exec.Command("mysqlctl", "-log_dir", dataRoot+"/tmp", "-tablet_uid", fmt.Sprint(uid), "-mysql_port", fmt.Sprint(mysqlPort), "init", "-init_db_sql_file", path.Join(root, "/config/init_db.sql"), "start")
// 	proc.Stderr = os.Stderr
// 	proc.Stdout = os.Stdout
// 	return proc.Run()
// }

// // SetupVTTablet starts vttablet corresponds to dbName and uid.
// func SetupVTTablet(dbName string, uid int) error {
// 	// start vttablet
// 	fmt.Println("Starting Vttablet", dbName)

// 	shard := "0"
// 	port := 15000 + uid
// 	grpcPort := 16000 + uid
// 	alias := fmt.Sprintf("%s-%010d", cell, uid)
// 	tabletDir := fmt.Sprintf("vt_%010d", uid)
// 	tabletHostname := ""
// 	tabletLogfile := fmt.Sprintf("vttablet_%010d_querylog.txt", uid)
// 	tabletType := "replica"
// 	err := exec.Command(
// 		"vttablet",
// 		"-topo_implementation", *topoImplementation,
// 		"-topo_global_server_address", *topoGlobalServerAddress,
// 		"-topo_global_root", *topoGlobalRoot,
// 		"-log_dir", dataRoot+"/tmp",
// 		"-log_queries_to_file", dataRoot+"/tmp/"+tabletLogfile,
// 		"-tablet-path", alias,
// 		"-tablet_hostname", tabletHostname,
// 		"-init_keyspace", dbName,
// 		"-init_shard", shard,
// 		"-init_tablet_type", tabletType,
// 		"-health_check_interval", "5s",
// 		"-enable_semi_sync",
// 		"-enable_replication_reporter",
// 		"-backup_storage_implementation", "file",
// 		"-file_backup_storage_root", dataRoot+"/backups",
// 		"-restore_from_backup",
// 		"-port", fmt.Sprint(port),
// 		"-grpc_port", fmt.Sprint(grpcPort),
// 		"-service_map", "grpc-queryservice,grpc-tabletmanager,grpc-updatestream",
// 		"-pid_file", dataRoot+"/"+tabletDir+"/vttablet.pid",
// 		"-vtctld_addr", "http://$hostname:$vtctld_web_port/",
// 	).Start()
// 	if err != nil {
// 		return errors.Wrap(err, "starting vttablet")
// 	}

// 	return nil
// }
