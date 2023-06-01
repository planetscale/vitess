module vitess.io/vitess

go 1.20

require (
	cloud.google.com/go/storage v1.29.0
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20230106234847-43070de90fa1
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/DataDog/datadog-go v4.8.3+incompatible
	github.com/HdrHistogram/hdrhistogram-go v0.9.0 // indirect
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/aquarapid/vaultlib v0.5.1
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/aws/aws-sdk-go v1.44.217
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/corpix/uarand v0.1.1 // indirect
	github.com/dave/jennifer v1.6.0
	github.com/evanphx/json-patch v5.6.0+incompatible
	github.com/fsnotify/fsnotify v1.6.0
	github.com/go-sql-driver/mysql v1.7.0
	github.com/golang/glog v1.1.0
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.3
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.5.9
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.3.0
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/consul/api v1.20.0
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/icrowley/fake v0.0.0-20180203215853-4178557ae428
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.16.5
	github.com/klauspost/pgzip v1.2.5
	github.com/krishicks/yaml-patch v0.0.10
	github.com/magiconair/properties v1.8.7
	github.com/minio/minio-go v0.0.0-20190131015406-c8a261de75c1
	github.com/montanaflynn/stats v0.7.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing/opentracing-go v1.2.0
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/philhofer/fwd v1.1.2 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pires/go-proxyproto v0.6.2
	github.com/pkg/errors v0.9.1
	github.com/planetscale/common-libs v0.2.1
	github.com/planetscale/pargzip v0.0.0-20201116224723-90c7fc03ea8a
	github.com/planetscale/vtprotobuf v0.4.0
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/common v0.42.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/sjmudd/stopwatch v0.1.1
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.6.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.15.0
	github.com/stretchr/testify v1.8.2
	github.com/tchap/go-patricia v2.3.0+incompatible
	github.com/tidwall/gjson v1.12.1
	github.com/tinylib/msgp v1.1.8 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82
	github.com/z-division/go-zookeeper v1.0.0
	go.etcd.io/etcd/api/v3 v3.5.7
	go.etcd.io/etcd/client/pkg/v3 v3.5.7
	go.etcd.io/etcd/client/v3 v3.5.7
	golang.org/x/crypto v0.7.0 // indirect
	golang.org/x/mod v0.10.0 // indirect
	golang.org/x/net v0.9.0
	golang.org/x/oauth2 v0.6.0
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/term v0.7.0
	golang.org/x/text v0.9.0
	golang.org/x/time v0.3.0
	golang.org/x/tools v0.8.0
	google.golang.org/api v0.111.0
	google.golang.org/genproto v0.0.0-20230306155012-7f2fa6fef1f4 // indirect
	google.golang.org/grpc v1.53.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.3.0
	google.golang.org/grpc/examples v0.0.0-20210430044426-28078834f35b
	google.golang.org/protobuf v1.29.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.48.0
	gopkg.in/asn1-ber.v1 v1.0.0-20181015200546-f715ec2f112d // indirect
	gopkg.in/gcfg.v1 v1.2.3
	gopkg.in/ldap.v2 v2.5.1
	gopkg.in/warnings.v0 v0.1.2 // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/apiextensions-apiserver v0.18.19
	k8s.io/apimachinery v0.26.2
	k8s.io/client-go v0.26.2
	k8s.io/code-generator v0.26.2
	sigs.k8s.io/yaml v1.3.0
)

require github.com/bndr/gotabulate v1.1.2

require github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7

require (
	github.com/Shopify/toxiproxy/v2 v2.5.0
	github.com/cespare/xxhash v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/google/safehtml v0.1.0
	github.com/hashicorp/go-version v1.6.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/kr/pretty v0.3.1
	github.com/kr/text v0.2.0
	github.com/lestrrat-go/strftime v1.0.6
	github.com/mitchellh/hashstructure v1.1.0
	github.com/nsf/jsondiff v0.0.0-20210926074059-1e845ec5d249
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/planetscale/psevents v0.0.0-20230523165553-cab767299381
	github.com/pmezard/go-difflib v1.0.0
	github.com/segmentio/fasthash v1.0.3
	github.com/segmentio/kafka-go v0.4.39
	github.com/shopspring/decimal v1.3.1
	github.com/tidwall/btree v1.6.0
	github.com/twmb/murmur3 v1.1.6
	github.com/xlab/treeprint v1.2.0
	go.opentelemetry.io/otel v1.14.0
	go.opentelemetry.io/otel/trace v1.14.0
	go.uber.org/goleak v1.2.1
	go.uber.org/multierr v1.10.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20230307190834-24139beb5833
	golang.org/x/sync v0.1.0
	gopkg.in/src-d/go-errors.v1 v1.0.0
	k8s.io/utils v0.0.0-20230308161112-d77c459e9343
	modernc.org/sqlite v1.21.0
	storj.io/drpc v0.0.33
)

require (
	cloud.google.com/go v0.110.0 // indirect
	cloud.google.com/go/compute v1.18.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.12.0 // indirect
	github.com/DataDog/datadog-agent/pkg/obfuscate v0.43.1 // indirect
	github.com/DataDog/datadog-agent/pkg/remoteconfig/state v0.43.1 // indirect
	github.com/DataDog/datadog-go/v5 v5.3.0 // indirect
	github.com/DataDog/go-tuf v0.3.0--fix-localmeta-fork // indirect
	github.com/DataDog/sketches-go v1.4.1 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/andybalholm/cascadia v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/cyphar/filepath-securejoin v0.2.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emicklei/go-restful/v3 v3.10.2 // indirect
	github.com/fatih/color v1.14.1 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/go-logr/logr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/gnostic v0.6.9 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.4.0 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.10 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/outcaste-io/ristretto v0.2.1 // indirect
	github.com/pelletier/go-toml/v2 v2.0.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/secure-systems-lab/go-securesystemslib v0.5.0 // indirect
	github.com/spf13/afero v1.9.5 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/subosito/gotenv v1.4.2 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/xdg/scram v1.0.5 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/zeebo/errs v1.3.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go4.org/intern v0.0.0-20230205224052-192e9f60865c // indirect
	go4.org/unsafe/assume-no-moving-gc v0.0.0-20230221090011-e4bae7ad2296 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	inet.af/netaddr v0.0.0-20220811202034-502d2d690317 // indirect
	k8s.io/api v0.26.2 // indirect
	k8s.io/gengo v0.0.0-20230306165830-ab3349d207d4 // indirect
	k8s.io/klog/v2 v2.90.1 // indirect
	k8s.io/kube-openapi v0.0.0-20230308215209-15aac26d736a // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.40.0 // indirect
	modernc.org/ccgo/v3 v3.16.13 // indirect
	modernc.org/libc v1.22.3 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.5.0 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.1.0 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
)

replace github.com/oliveagle/jsonpath => github.com/dolthub/jsonpath v0.0.0-20210609232853-d49537a30474

exclude github.com/go-logr/logr v1.2.3
