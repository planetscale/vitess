name: {{.Name}}
on: [push, pull_request]
concurrency:
  group: format('{0}-{1}', ${{"{{"}} github.ref {{"}}"}}, '{{.Name}}')
  cancel-in-progress: true

jobs:
  test:
    runs-on: ubuntu-20.04

    steps:
    - name: Check if workflow needs to be skipped
      id: skip-workflow
      run: |
        skip='false'
        if [[ "{{"${{github.event.pull_request}}"}}" ==  "" ]] && [[ "{{"${{github.ref}}"}}" != "refs/heads/main" ]] && [[ ! "{{"${{github.ref}}"}}" =~ ^refs/heads/release-[0-9]+\.[0-9]$ ]] && [[ ! "{{"${{github.ref}}"}}" =~ "refs/tags/.*" ]]; then
          skip='true'
        fi
        echo Skip ${skip}
        echo "::set-output name=skip-workflow::${skip}"

    - name: Check out code
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: actions/checkout@v2

    - name: Check for changes in relevant files
      if: steps.skip-workflow.outputs.skip-workflow == 'false'
      uses: frouioui/paths-filter@main
      id: changes
      with:
        token: ''
        filters: |
          unit_tests:
            - 'go/**'
            - 'test.go'
            - 'Makefile'
            - 'build.env'
            - 'go.[sumod]'
            - 'proto/*.proto'
            - 'tools/**'
            - 'config/**'
            - 'bootstrap.sh'
            - '.github/workflows/{{.FileName}}'

    - name: Set up Go
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      uses: actions/setup-go@v2
      with:
        go-version: 1.18.7

    - name: Tune the OS
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      run: |
        echo '1024 65535' | sudo tee -a /proc/sys/net/ipv4/ip_local_port_range
        # Increase the asynchronous non-blocking I/O. More information at https://dev.mysql.com/doc/refman/5.7/en/innodb-parameters.html#sysvar_innodb_use_native_aio
        echo "fs.aio-max-nr = 1048576" | sudo tee -a /etc/sysctl.conf
        sudo sysctl -p /etc/sysctl.conf

    - name: Get dependencies
      env: # Or as an environment variable
        AWS_ACCESS_KEY_ID: ${{"{{"}} secrets.BUILDKITE_S3_ACCESS_KEY_ID {{"}}"}}
        AWS_SECRET_ACCESS_KEY: ${{"{{"}} secrets.BUILDKITE_S3_SECRET_ACCESS_KEY {{"}}"}}
        AWS_DEFAULT_REGION: us-east-1
      run: |
        export DEBIAN_FRONTEND="noninteractive"
        sudo apt-get update
        # stop any existing running instance of mysql
        sudo service mysql stop
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld

        # Uninstall any previously installed MySQL first
        sudo DEBIAN_FRONTEND="noninteractive" apt-get remove -y --purge mysql-server mysql-client mysql-common
        sudo apt-get -y autoremove
        sudo apt-get -y autoclean
        sudo deluser mysql
        sudo rm -rf /var/lib/mysql
        sudo rm -rf /etc/mysql

        # install necessary tools
        sudo apt-get install -y make unzip g++ curl git wget awscli ant openjdk-8-jdk

        # Get latest version of mysql from s3 bucket
        LATEST_BUILD=$(aws s3api list-objects-v2 --bucket "planetscale-mysql-server-private-ci-artifacts" --prefix mysql/main/dist --query 'reverse(sort_by(Contents[?contains(Key, `focal`)], &LastModified))[:1].Key' --output=text)
        echo "latest build is $LATEST_BUILD"
        # Pin this to 8.0.30
        LAST_BUILD="mysql/main/dist/mysql-8.0.30.20221013-ps-87805bf371e-focal-linux-x86_64.tar.gz"
        aws s3 cp "s3://planetscale-mysql-server-private-ci-artifacts/${LAST_BUILD}" .
        sudo tar xf $(basename $LAST_BUILD) -v -C /usr --strip-components=1

        # Install everything else we need, and configure
        sudo apt-get install -y mysql-server mysql-client eatmydata xz-utils
        sudo service mysql stop
        sudo bash -c "echo '/usr/sbin/mysqld { }' > /etc/apparmor.d/usr.sbin.mysqld" # https://bugs.launchpad.net/ubuntu/+source/mariadb-10.1/+bug/1806263
        sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
        sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld || echo "could not remove mysqld profile"

        mkdir -p dist bin
        curl -L https://github.com/coreos/etcd/releases/download/v3.3.10/etcd-v3.3.10-linux-amd64.tar.gz | tar -zxC dist
        mv dist/etcd-v3.3.10-linux-amd64/{etcd,etcdctl} bin/

        go mod download
        go install golang.org/x/tools/cmd/goimports@latest

    - name: Run make tools
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      run: |
        make tools

    - name: Run test
      if: steps.skip-workflow.outputs.skip-workflow == 'false' && steps.changes.outputs.unit_tests == 'true'
      timeout-minutes: 30
      run: |
        eatmydata -- make unit_test
