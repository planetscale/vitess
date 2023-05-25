/*
Copyright 2022 The Vitess Authors.

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

package vtgate

import (
	"context"
	"crypto/tls"
	"flag"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"vitess.io/vitess/go/vt/vtgate/logstats"

	"vitess.io/vitess/go/vt/vtgate/errorsanitizer"

	"github.com/segmentio/kafka-go"

	"vitess.io/vitess/go/vt/sqlparser"

	"google.golang.org/protobuf/encoding/prototext"

	"github.com/twmb/murmur3"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	pbenvelope "github.com/planetscale/psevents/go/v1"
	pbvtgate "github.com/planetscale/psevents/go/vtgate/v1"
	"github.com/segmentio/kafka-go/sasl/scram"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/log"
	vtmath "vitess.io/vitess/go/vt/vtorc/util"
)

const (
	queryTopic            = "vtgate.v1.Query"
	queryStatsBundleTopic = "vtgate.v1.QueryStatsBundle"
	schemaChangeTopic     = "vtgate.v1.SchemaChange"
	queryURLBase          = "psevents.planetscale.com"
	BrokersVar            = "INSIGHTS_KAFKA_BROKERS"
	UsernameVar           = "INSIGHTS_KAFKA_USERNAME"
	PasswordVar           = "INSIGHTS_KAFKA_PASSWORD"
)

// Two-tiered rate limiter, used to limit produce rate of Query messages. Overall rate is held to the limit/burst
// specified in the global limiter, but prevent clients from immediately burning through the burst capacity by only
// allowing #intervalMax per interval.
type limiter struct {
	global            *rate.Limiter
	intervalMax       int
	intervalRemain    int
	intervalAllotment int // The original number of messages that can be allowed in this interval, used for logging
}

func (lim *limiter) allow() bool {
	if lim.intervalRemain > 0 {
		lim.intervalRemain--
		return true
	}

	return false
}

// Advance to the next interval, pulling tokens from the global limiter as necessary
func (lim *limiter) tick() {
	lim.tickAt(time.Now())
}

func (lim *limiter) tickAt(t time.Time) {
	take := lim.intervalMax - lim.intervalRemain

	available := int(lim.global.TokensAt(t))
	if take > available {
		take = available
	}
	lim.global.AllowN(t, take)
	lim.intervalRemain += take
	lim.intervalAllotment = lim.intervalRemain
}

type QueryPatternAggregation struct {
	QueryCount         uint64
	ErrorCount         uint64
	SumShardQueries    uint64
	MaxShardQueries    uint64
	SumRowsRead        uint64
	MaxRowsRead        uint64
	SumRowsAffected    uint64
	MaxRowsAffected    uint64
	SumRowsReturned    uint64
	MaxRowsReturned    uint64
	SumTotalDuration   time.Duration
	MaxTotalDuration   time.Duration
	SumPlanDuration    time.Duration
	MaxPlanDuration    time.Duration
	SumExecuteDuration time.Duration
	MaxExecuteDuration time.Duration
	SumCommitDuration  time.Duration
	MaxCommitDuration  time.Duration

	// DDSketch groups observed values into buckets of exponentially increasing width suitable for determining
	// relative-accuracy quantiles.
	//
	// Approach based on https://arxiv.org/pdf/1908.10693.pdf. Only records positive values and doesn't do any bucket
	// collapsing.
	TotalDurationSketchBuckets map[uint32]uint32

	StatementType string
	TablesUsed    []string
}

type QueryPatternKey struct {
	ActiveKeyspace     string
	SQL                string
	BoostQueryPublicID string
	TabletType         string
}

type Insights struct {
	// configuration
	DatabaseBranchPublicID    string
	Brokers                   []string
	Username                  string
	Password                  string
	MaxInFlight               uint
	Interval                  time.Duration
	MaxPatterns               uint
	RowsReadThreshold         uint
	ResponseTimeThreshold     uint
	KafkaText                 bool // use human-readable pb, for tests and debugging
	SendRawQueries            bool
	MaxRawQueryLength         uint
	ReorderThreshold          uint // alphabetize columns if >= this many redundant patterns in 15s
	SendTotalDurationSketches bool

	// Sketches
	TotalDurationSketchAlpha           float32
	TotalDurationSketchUnits           int
	TotalDurationSketchUnitsMultiplier float64
	TotalDurationSketchGamma           float64
	TotalDurationSketchLogGamma        float64

	// state
	KafkaWriter          *kafka.Writer
	Aggregations         map[QueryPatternKey]*QueryPatternAggregation
	PeriodStart          time.Time
	InFlightCounter      uint64
	Timer                *time.Ticker
	LogChan              chan *logstats.LogStats
	Workers              sync.WaitGroup
	QueriesThisInterval  uint
	ReorderInsertColumns bool
	ColIndependentHashes map[uint32]uint

	// log state: we limit some log messages to once per 15s because they're caused by behavior the
	// client controls
	LogPatternsExceeded   uint
	LogBufferExceeded     uint
	LogMaxQueriesExceeded uint

	// hooks
	Sender func([]byte, string, string) error

	LogQueriesLimiter limiter
}

var (
	// insightsKafkaBrokers specifies a comma-separated list of host:port endpoints where Insights metrics are sent to Kafka.
	// If omitted (the default), no Insights metrics are sent.
	insightsKafkaBrokers = flag.String("insights_kafka_brokers", "", "Enable Kafka metrics to the given host:port endpoint")

	// insightsKafkaUsername specifies the username sent to authenticate to the Kafka endpoint
	insightsKafkaUsername = flag.String("insights_kafka_username", "", "Username for the Kafka endpoint")

	// insightsKafkaPassword specifies the password sent to authenticate to the Kafka endpoint
	insightsKafkaPassword = flag.String("insights_kafka_password", "", "Password for the Kafka endpoint")

	// insightsKafkaBufferSize in a cap on the message payload bytes that can be inflight (i.e. not yet sent to Kafka) before we start dropping messages to avoid unbounded memory usage if Kafka is down/slow
	insightsKafkaBufferSize = flag.Uint("insights_kafka_buffer", 5*1024*1024, "Maximum memory dedicated to unsent Kafka messages; above this threshold, messages will be dropped")

	// insightsRTThreshold is the response-time threshold in milliseconds, above which individual queries are reported
	insightsRTThreshold = flag.Uint("insights_rt_threshold", 1000, "Report individual queries that take at least this many milliseconds")

	// insightsRowsReadThreshold is the threshold on the number of rows read (scanned) above which individual queries are reported
	insightsRowsReadThreshold = flag.Uint("insights_rows_read_threshold", 10000, "Report individual transactions that read (scan) at least this many rows")

	// insightsQueriesLimitierRate is the average rate of the token bucket limiter on individual query messages sent to insights
	insightsQueriesLimiterRate = flag.Float64("insights_queries_limiter_rate", 0.1, "Limit on (average) queries reported per second")

	// insightsQueriesLimitierBurst is the burst capacity of the token bucket limiter on individual query messages sent to insights
	insightsQueriesLimiterBurst = flag.Uint("insights_queries_limiter_burst", 2000, "Burst limit on individual queries reported")

	// insightsMaxQueriesPerInterval is the maximum number of individual queries that can be reported per interval.  Interesting queries above this threshold
	// will simply not be reported until the next interval begins.
	insightsMaxQueriesPerInterval = flag.Uint("insights_max_queries_per_interval", 100, "Limit on individual queries reported per flush interval")

	// insightsFlushInterval is how often, in seconds, to send aggregated metrics for query patterns
	insightsFlushInterval = flag.Uint("insights_flush_interval", 15, "Send aggregated metrics for all query patterns every N seconds")

	// insightsPatternLimit is the maximum number of query patterns to track between flushes.  The first N patterns are tracked, and anything beyond
	// that is silently dropped until the next flush time.
	insightsPatternLimit = flag.Uint("insights_pattern_limit", 1000, "Maximum number of unique patterns to track in a flush interval")

	// databaseBranchPublicID is api-bb's name for the database branch this cluster hosts
	databaseBranchPublicID = flag.String("database_branch_public_id", "", "The public ID of the database branch this cluster hosts, used for Insights")

	// insightsKafkaText is true if we should send protobufs message in clear text, for unit tests and debugging
	insightsKafkaText = flag.Bool("insights_kafka_text", false, "Send Insights messages as plain text")

	// insightsRawQueries is true if we should send raw, unnormalized queries as part of Kafka "Query" messages
	insightsRawQueries = flag.Bool("insights_raw_queries", false, "Send unnormalized SQL for individually reported queries")

	// insightsRawQueriesMaxLength is the longest string, in bytes, we will send as the RawSql field in a Kafka message
	insightsRawQueriesMaxLength = flag.Uint("insights_raw_queries_max_length", 8192, "Maximum size for unnormalized SQL")

	// normalize more aggressively by alphabetizing INSERT columns if there are more than this many
	// otherwise identical patterns in a single interval
	insightsReorderThreshold = flag.Uint("insights_reorder_threshold", 5, "Reorder INSERT columns if more than this many redundant patterns in an interval")

	insightsTotalDurationSketches    = flag.Bool("insights_total_duration_sketches", false, "Send quantile sketches for total query duration")
	insightsTotalDurationSketchAlpha = flag.Float64("insights_total_duration_sketch_alpha", 0.01, "Relative accuracy for total query duration sketches")
	insightsTotalDurationSketchUnits = flag.Int("insights_total_duration_sketch_units", -6, "Base 10 exponent for the units of the total query duration sketches, relative to seconds (0 = seconds, -3 = milliseconds, -6 = microseconds)")
)

func initInsights(logger *streamlog.StreamLogger[*logstats.LogStats]) (*Insights, error) {
	return initInsightsInner(logger,
		argOrEnv(*insightsKafkaBrokers, BrokersVar),
		*databaseBranchPublicID,
		argOrEnv(*insightsKafkaUsername, UsernameVar),
		argOrEnv(*insightsKafkaPassword, PasswordVar),
		*insightsKafkaBufferSize,
		*insightsPatternLimit,
		*insightsRowsReadThreshold,
		*insightsRTThreshold,
		*insightsMaxQueriesPerInterval,
		*insightsQueriesLimiterRate,
		*insightsQueriesLimiterBurst,
		*insightsRawQueriesMaxLength,
		*insightsReorderThreshold,
		time.Duration(*insightsFlushInterval)*time.Second,
		*insightsKafkaText,
		*insightsRawQueries,
		*insightsTotalDurationSketches,
		*insightsTotalDurationSketchAlpha,
		*insightsTotalDurationSketchUnits,
	)
}

func initInsightsInner(logger *streamlog.StreamLogger[*logstats.LogStats],
	brokers, publicID, username, password string,
	bufsize, patternLimit, rowsReadThreshold, responseTimeThreshold uint,
	maxQueriesPerInterval uint,
	insightsQueriesLimiterRate float64,
	insightsQueriesLimiterBurst uint,
	maxRawQueryLength, reorderThreshold uint,
	interval time.Duration,
	kafkaText, sendRawQueries, sendTotalDurationSketches bool,
	totalDurationSketchAlpha float64,
	totalDurationSketchUnits int) (*Insights, error) {

	if brokers == "" {
		return nil, nil
	}

	if publicID == "" {
		return nil, errors.New("-database_branch_public_id is required if Insights is enabled")
	}

	alpha := float32(totalDurationSketchAlpha)

	if alpha <= 0.0 || alpha >= 1.0 {
		return nil, errors.New("-insights_total_duration_sketch_alpha must be between 0.0 and 1.0 (exclusive)")
	}

	if totalDurationSketchUnits < -9 || totalDurationSketchUnits > 0 {
		return nil, errors.New("-insights_total_duration_sketch_units must be between -9 and 0 (inclusive)")
	}

	gamma := (1 + totalDurationSketchAlpha) / (1 - totalDurationSketchAlpha)

	limiter := limiter{
		global:      rate.NewLimiter(rate.Limit(insightsQueriesLimiterRate), int(insightsQueriesLimiterBurst)),
		intervalMax: int(maxQueriesPerInterval),
	}

	insights := Insights{
		DatabaseBranchPublicID:             publicID,
		Brokers:                            strings.Split(brokers, ","),
		Username:                           username,
		Password:                           password,
		MaxInFlight:                        bufsize,
		Interval:                           interval,
		MaxPatterns:                        patternLimit,
		RowsReadThreshold:                  rowsReadThreshold,
		ResponseTimeThreshold:              responseTimeThreshold,
		LogQueriesLimiter:                  limiter,
		KafkaText:                          kafkaText,
		SendRawQueries:                     sendRawQueries,
		MaxRawQueryLength:                  maxRawQueryLength,
		ReorderThreshold:                   reorderThreshold,
		SendTotalDurationSketches:          sendTotalDurationSketches,
		TotalDurationSketchAlpha:           alpha,
		TotalDurationSketchUnits:           totalDurationSketchUnits,
		TotalDurationSketchUnitsMultiplier: math.Pow10(-9 - totalDurationSketchUnits),
		TotalDurationSketchGamma:           gamma,
		TotalDurationSketchLogGamma:        math.Log(gamma),
	}
	insights.Sender = insights.sendToKafka
	err := insights.logToKafka(logger)
	if err != nil {
		return nil, err
	}

	return &insights, nil
}

func (ii *Insights) Drain() bool {
	if ii.LogChan == nil {
		return true
	}

	close(ii.LogChan)
	ii.Workers.Wait()
	return ii.LogChan == nil
}

func argOrEnv(argVal, envKey string) string {
	if argVal != "" {
		return argVal
	}
	return os.Getenv(envKey)
}

func (ii *Insights) newPatternAggregation(statementType string, tablesUsed []string) *QueryPatternAggregation {
	agg := QueryPatternAggregation{
		StatementType: statementType,
		TablesUsed:    tablesUsed,
	}

	if ii.SendTotalDurationSketches {
		agg.TotalDurationSketchBuckets = make(map[uint32]uint32)
	}

	return &agg
}

func (ii *Insights) startInterval() {
	ii.LogQueriesLimiter.tick()
	ii.QueriesThisInterval = 0
	ii.Aggregations = make(map[QueryPatternKey]*QueryPatternAggregation)
	ii.PeriodStart = time.Now()
	ii.ColIndependentHashes = make(map[uint32]uint)
}

func (ii *Insights) shouldSendToInsights(ls *logstats.LogStats) bool {
	return ls.TotalTime().Milliseconds() > int64(ii.ResponseTimeThreshold) || ls.RowsRead >= uint64(ii.RowsReadThreshold) || ls.Error != nil
}

func (ii *Insights) logToKafka(logger *streamlog.StreamLogger[*logstats.LogStats]) error {
	var transport kafka.RoundTripper

	if ii.Username != "" && ii.Password != "" {
		t := &kafka.Transport{
			TLS: &tls.Config{},
		}

		mechanism, err := scram.Mechanism(scram.SHA512, ii.Username, ii.Password)
		if err != nil {
			return errors.Wrap(err, "kafka scram configuration failed")
		}

		t.SASL = mechanism
		transport = t
	} else if ii.Username != "" {
		return errors.New("kafka username specified without a password")
	} else if ii.Password != "" {
		return errors.New("kafka password specified without a username")
	} else {
		transport = kafka.DefaultTransport
	}

	ii.KafkaWriter = &kafka.Writer{
		// This should be set to the bootstrap brokers (which the MSK interface provides)
		Addr:        kafka.TCP(ii.Brokers...),
		Balancer:    &kafka.Murmur2Balancer{},
		Transport:   transport,
		Compression: kafka.Snappy,
		// Setting acks=1, so that we lose fewer messages when partition leadership changes (which happens when a broker restarts, for example).
		// If we find ourselves unable to produce messages quickly enough, we can set this to kafka.None
		RequiredAcks: kafka.RequireOne,
		Async:        true,

		// Not setting Logger, because it records one log line per message
		ErrorLogger: kafka.LoggerFunc(log.Errorf),
		Completion: func(messages []kafka.Message, err error) {
			release := 0
			for _, m := range messages {
				release += len(m.Value)
			}
			atomic.AddUint64(&ii.InFlightCounter, -uint64(release))

			if err != nil {
				// log or increment failed send counter with len(messages)
				log.Warningf("Could not send %d-byte message to Kafka: %v", release, err)
			}
		},
	}

	ii.startInterval()

	ii.LogChan = logger.Subscribe("Kafka")
	ii.Workers.Add(1)
	ii.Timer = time.NewTicker(ii.Interval)
	go func() {
		defer func() {
			logger.Unsubscribe(ii.LogChan)
			ii.LogChan = nil
			ii.Timer.Stop()
			ii.Workers.Done()
		}()
		for {
			select {
			case record, ok := <-ii.LogChan:
				if !ok {
					// eof means someone called Drain to kill this worker
					return
				}
				if record == nil {
					// unit tests send a nil record to emulate a 15s heartbeat
					ii.sendAggregates()
				} else {
					ii.handleMessage(record)
				}
			case <-ii.Timer.C:
				ii.sendAggregates()
			}
		}
	}()

	return nil
}

func (ii *Insights) makeSchemaChangeMessage(ls *logstats.LogStats) ([]byte, error) {
	stmt, err := sqlparser.Parse(ls.SQL)

	if err != nil {
		return nil, err
	}

	ddlStmt, ok := stmt.(sqlparser.DDLStatement)
	if !ok {
		return nil, errors.Errorf("Expected a DDLStatement but got a %T", stmt)
	}

	operation := pbvtgate.SchemaChange_UNKNOWN

	switch ddlStmt.(type) {
	case *sqlparser.CreateTable:
		operation = pbvtgate.SchemaChange_CREATE_TABLE
	case *sqlparser.AlterTable:
		operation = pbvtgate.SchemaChange_ALTER_TABLE
	case *sqlparser.TruncateTable:
		// Truncate table isn't really a schema change
		return nil, nil
	case *sqlparser.DropTable:
		operation = pbvtgate.SchemaChange_DROP_TABLE
	case *sqlparser.RenameTable:
		operation = pbvtgate.SchemaChange_RENAME_TABLE
	case *sqlparser.AlterView:
		operation = pbvtgate.SchemaChange_ALTER_VIEW
	case *sqlparser.CreateView:
		operation = pbvtgate.SchemaChange_CREATE_VIEW
	case *sqlparser.DropView:
		operation = pbvtgate.SchemaChange_DROP_VIEW
	}

	// Sometimes DDL statements aren't fully parsed (but are still executed).
	// If that happens we want to send along the original query as the DDL.
	fullyParsed := ddlStmt.IsFullyParsed()
	var ddl string
	if fullyParsed {
		ddl = sqlparser.CanonicalString(ddlStmt)
	} else {
		ddl = ls.SQL
	}

	sc := pbvtgate.SchemaChange{
		DatabaseBranchPublicId: ii.DatabaseBranchPublicID,
		Ddl:                    ddl,
		Normalized:             fullyParsed,
		Operation:              operation,
	}

	var out []byte

	if ii.KafkaText {
		out, err = prototext.Marshal(&sc)
	} else {
		out, err = sc.MarshalVT()
	}
	if err != nil {
		return nil, err
	}

	return ii.makeEnvelope(out, schemaChangeTopic)
}

func (ii *Insights) handleMessage(record any) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("PANIC while processing Insights message: %v", r)
		}
	}()

	ls, ok := record.(*logstats.LogStats)
	if !ok {
		log.Infof("not a LogStats: %v (%T)", record, record)
		return
	}

	if ls.Method == "Prepare" {
		return // Don't record prepares
	}

	if ls.StmtType == "DDL" && ls.Error == nil {
		buf, err := ii.makeSchemaChangeMessage(ls)

		if err != nil {
			log.Warningf("Could not send schema change event: %v", err)
		} else {
			if buf != nil {
				ii.reserveAndSend(buf, schemaChangeTopic, ii.DatabaseBranchPublicID)
			}
		}
	}

	var sql string
	var comments []string
	var ciHash *uint32
	if (ls.IsNormalized || ls.Error == nil) && ls.AST != nil {
		comments = extractComments(ls.SQL)
		sql, ciHash = ii.normalizeSQL(ls.AST, ls.StmtType == "INSERT")
	} else {
		sql = "<error>"
	}
	if ls.Error != nil && ls.StmtType == "" {
		ls.StmtType = "ERROR"
	}

	ii.addToAggregates(ls, sql, ciHash)

	if !ii.shouldSendToInsights(ls) {
		return
	}

	if !ii.LogQueriesLimiter.allow() {
		ii.LogMaxQueriesExceeded++
		return
	}

	buf, err := ii.makeQueryMessage(ls, sql, parseCommentTags(comments))
	if err != nil {
		log.Warningf("Could not serialize %s message: %v", queryTopic, err)
	} else {
		var kafkaKey string
		if ls.Error != nil {
			kafkaKey = ii.makeKafkaKey(ls.Error.Error())
		} else {
			kafkaKey = ii.makeKafkaKey(sql)
		}
		if ii.reserveAndSend(buf, queryTopic, kafkaKey) {
			ii.QueriesThisInterval++
		}
	}
}

func (ii *Insights) makeKafkaKey(sql string) string {
	h := murmur3.Sum32([]byte(sql))
	return ii.DatabaseBranchPublicID + "/" + strconv.FormatUint(uint64(h), 16)
}

func (ii *Insights) reserveAndSend(buf []byte, topic, key string) bool {
	reserve := uint64(len(buf))

	if atomic.LoadUint64(&ii.InFlightCounter)+reserve >= uint64(ii.MaxInFlight) {
		// log or increment a dropped message counter
		ii.LogBufferExceeded++
		return false
	}

	err := ii.Sender(buf, topic, key)
	if err != nil {
		log.Warningf("Error sending to Kafka: %v", err)
		return false
	}

	atomic.AddUint64(&ii.InFlightCounter, reserve)
	return true
}

func (ii *Insights) sendToKafka(buf []byte, topic, key string) error {
	return ii.KafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Topic: topic,
			Key:   []byte(key),
			Value: buf,
		})
}

func maxDuration(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (ii *Insights) addToAggregates(ls *logstats.LogStats, sql string, ciHash *uint32) bool {
	// no locks needed if all callers are on the same thread

	var pa *QueryPatternAggregation
	key := QueryPatternKey{
		ActiveKeyspace:     ls.ActiveKeyspace,
		BoostQueryPublicID: ls.BoostQueryID,
		TabletType:         ls.TabletType,
		SQL:                sql,
	}

	pa, ok := ii.Aggregations[key]
	if !ok {
		if uint(len(ii.Aggregations)) >= ii.MaxPatterns {
			ii.LogPatternsExceeded++
			return false
		}
		// ls.StmtType, ls.Table and ls.TablesUsed depend only on the contents of sql, so we don't separately make them
		// part of the key, and we don't track them as separate values in the QueryPatternAggregation values.
		// In other words, we assume they don't change, so we only need to track a single value for each.
		pa = ii.newPatternAggregation(ls.StmtType, ls.TablesUsed)
		ii.Aggregations[key] = pa

		if ciHash != nil && !ii.ReorderInsertColumns {
			ii.ColIndependentHashes[*ciHash]++
			if ii.ColIndependentHashes[*ciHash] >= ii.ReorderThreshold {
				log.Info("Enabling ReorderInsertColumns")
				ii.ReorderInsertColumns = true
			}
		}
	}

	pa.QueryCount++
	if ls.Error != nil {
		pa.ErrorCount++
	}
	pa.SumShardQueries += ls.ShardQueries
	pa.MaxShardQueries = vtmath.MaxUInt64(pa.MaxShardQueries, ls.ShardQueries)
	pa.SumRowsRead += ls.RowsRead
	pa.MaxRowsRead = vtmath.MaxUInt64(pa.MaxRowsRead, ls.RowsRead)
	pa.SumRowsAffected += ls.RowsAffected
	pa.MaxRowsAffected = vtmath.MaxUInt64(pa.MaxRowsAffected, ls.RowsAffected)
	pa.SumRowsReturned += ls.RowsReturned
	pa.MaxRowsReturned = vtmath.MaxUInt64(pa.MaxRowsReturned, ls.RowsReturned)
	pa.SumTotalDuration += ls.TotalTime()
	pa.MaxTotalDuration = maxDuration(pa.MaxTotalDuration, ls.TotalTime())
	pa.SumPlanDuration += ls.PlanTime
	pa.MaxPlanDuration = maxDuration(pa.MaxPlanDuration, ls.PlanTime)
	pa.SumExecuteDuration += ls.ExecuteTime
	pa.MaxExecuteDuration = maxDuration(pa.MaxExecuteDuration, ls.ExecuteTime)
	pa.SumCommitDuration += ls.CommitTime
	pa.MaxCommitDuration = maxDuration(pa.MaxCommitDuration, ls.CommitTime)

	if ii.SendTotalDurationSketches {
		unitDuration := float64(ls.TotalTime()) * ii.TotalDurationSketchUnitsMultiplier

		// Round everything under 1 to 1 to avoid negative bucket indexes
		if unitDuration < 1.0 {
			unitDuration = 1.0
		}

		bucket := uint32(math.Ceil(math.Log(unitDuration) / ii.TotalDurationSketchLogGamma))
		pa.TotalDurationSketchBuckets[bucket]++
	}

	return true
}

func (ii *Insights) sendAggregates() {
	// no locks needed if all callers are on the same thread

	if ii.LogPatternsExceeded > 0 {
		log.Infof("Too many patterns: reached limit of %v.  %v statements not aggregated.", ii.MaxPatterns, ii.LogPatternsExceeded)
		ii.LogPatternsExceeded = 0
	}
	if ii.LogMaxQueriesExceeded > 0 {
		log.Infof("Too many queries: reached interval limit of %v.  %v statements not reported.", ii.LogQueriesLimiter.intervalAllotment, ii.LogMaxQueriesExceeded)
		ii.LogMaxQueriesExceeded = 0
	}

	for k, pa := range ii.Aggregations {
		buf, err := ii.makeQueryPatternMessage(k, pa)
		if err != nil {
			log.Warningf("Could not serialize %s message: %v", queryStatsBundleTopic, err)
		} else {
			ii.reserveAndSend(buf, queryStatsBundleTopic, ii.makeKafkaKey(k.SQL))
		}
	}

	if ii.LogBufferExceeded > 0 {
		log.Infof("Dropped %v Kafka message(s): InFlightCounter=%v, MaxInFlight=%v", ii.LogBufferExceeded, ii.InFlightCounter, ii.MaxInFlight)
		ii.LogBufferExceeded = 0
	}

	// remove all accumulated counters
	ii.startInterval()
}

func hostnameOrEmpty() string {
	hostname, err := os.Hostname()
	if err == nil {
		return ""
	}
	return hostname
}

func (ii *Insights) makeQueryMessage(ls *logstats.LogStats, sql string, tags []*pbvtgate.Query_Tag) ([]byte, error) {
	addr, user := ls.RemoteAddrUsername()
	// use the effective caller id if its present.
	// all queries sent to PlanetScale databases should have the effective caller-id set.
	if effectiveCaller := ls.EffectiveCaller(); effectiveCaller != "" {
		user = effectiveCaller
	}

	var port *wrapperspb.UInt32Value
	if strings.Contains(addr, ":") {
		tok := strings.SplitN(addr, ":", 2)
		p, err := strconv.ParseUint(tok[1], 10, 32)
		if err == nil {
			addr = tok[0]
			port = wrapperspb.UInt32(uint32(p))
		}
	}

	obj := pbvtgate.Query{
		StartTime:              timestamppb.New(ls.StartTime),
		DatabaseBranchPublicId: ii.DatabaseBranchPublicID,
		Username:               user,
		RemoteAddress:          stringOrNil(addr),
		RemotePort:             port,
		VtgateName:             hostnameOrEmpty(),
		NormalizedSql:          stringOrNil(sql),
		StatementType:          stringOrNil(ls.StmtType),
		TablesUsed:             ls.TablesUsed,
		ActiveKeyspace:         stringOrNil(ls.ActiveKeyspace),
		TabletType:             stringOrNil(ls.TabletType),
		ShardQueries:           uint32(ls.ShardQueries),
		RowsRead:               ls.RowsRead,
		RowsAffected:           ls.RowsAffected,
		RowsReturned:           ls.RowsReturned,
		TotalDuration:          durationOrNil(ls.TotalTime()),
		PlanDuration:           durationOrNil(ls.PlanTime),
		ExecuteDuration:        durationOrNil(ls.ExecuteTime),
		CommitDuration:         durationOrNil(ls.CommitTime),
		Error:                  stringOrNil(ls.ErrorStr()),
		CommentTags:            tags,
		BoostQueryPublicId:     stringOrNil(ls.BoostQueryID),
	}
	if ii.SendRawQueries {
		if ls.IsNormalized && ls.StmtType == "INSERT" {
			if s, err := shortenRawSQL(ls.RawSQL, ii.MaxRawQueryLength); err == nil {
				obj.RawSql = stringOrNil(s)
				obj.RawSqlAbbreviation = pbvtgate.Query_SUMMARIZED
			}
		}
		if obj.RawSql == nil { // not insert, not parseable, or insert that couldn't be summarized
			if len(ls.RawSQL) > int(ii.MaxRawQueryLength) {
				obj.RawSql = stringOrNil(efficientlyTruncate(ls.RawSQL, int(ii.MaxRawQueryLength)))
				obj.RawSqlAbbreviation = pbvtgate.Query_TRUNCATED
			} else {
				obj.RawSql = stringOrNil(ls.RawSQL)
			}
		}
	}
	if ls.Error != nil {
		obj.Error = stringOrNil(normalizeError(errorsanitizer.NormalizeError(ls.Error.Error())))
	}

	var out []byte
	var err error
	if ii.KafkaText {
		out, err = prototext.Marshal(&obj)
	} else {
		out, err = obj.MarshalVT()
	}
	if err != nil {
		return nil, err
	}
	return ii.makeEnvelope(out, queryTopic)
}

// efficientlyTruncate truncates a UTF-8 string at a character boundary, <= maxLength bytes long.
// This function is O(1), since it examines at most four bytes at the end of the string.
// If the string isn't valid UTF-8, the cut point is undefined and possibly different from
// safelyTruncate, cutting somewhere in the last four bytes of the string.
func efficientlyTruncate(str string, maxLength int) string {
	if len(str) <= maxLength {
		// short enough already
		return str
	}

	if str[maxLength]&0xc0 != 0x80 {
		// character after the cut isn't a multibyte continuation, so a cut at maxLength is clean
		return str[:maxLength]
	}

	str = str[:maxLength]
	idx := len(str) - 1
	left := vtmath.MaxInt(maxLength-3, 0)

	// rewind past any multibyte continuation
	for idx >= left && str[idx]&0xc0 == 0x80 {
		idx--
	}

	// rewind past the multibyte initiation
	if idx >= left && str[idx]&0xc0 == 0xc0 {
		return str[:idx]
	}

	// the sequence at the boundary wasn't a valid UTF-8 multibyte sequence, so cut at maxLength
	return str[:maxLength]
}

// safelyTruncate truncates a UTF-8 string at a character boundary, <= maxLength bytes long.
// This function is O(n) in the length of the string.
func safelyTruncate(str string, maxLength int) string {
	var lastIdx int
	for i := range str {
		if i > maxLength {
			return str[:lastIdx]
		}
		lastIdx = i
	}
	if len(str) > maxLength {
		return str[:lastIdx]
	}
	return str
}

// shortenRawSQL parses and reformulates the SQL statement to be shorter, while retaining more or less the
// same performance and EXPLAIN plan.  That's better than simply truncating the string, because the result
// should still be something we can pass to EXPLAIN.
//
// So far, the only technique we're using is to strip out all values
// but the first from INSERT statements.
func shortenRawSQL(rawSQL string, maxLength uint) (string, error) {
	stmt, err := sqlparser.Parse(rawSQL)
	if err != nil {
		// should never happen since the SQL was already processed
		return "", err
	}

	buf := sqlparser.NewTrackedBuffer(func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case sqlparser.Values:
			if len(node) < 1 {
				// unlikely, but just in case
				node.Format(buf)
			} else {
				buf.WriteString("values ")
				node[0].Format(buf)
			}
		default:
			node.Format(buf)
		}
	})
	ret := buf.WriteNode(stmt).String()
	if len(ret) > int(maxLength) {
		return "", errors.New("raw SQL string is still too long")
	}
	return ret, nil
}

func (ii *Insights) makeQueryPatternMessage(key QueryPatternKey, pa *QueryPatternAggregation) ([]byte, error) {
	obj := pbvtgate.QueryStatsBundle{
		PeriodStart:            timestamppb.New(ii.PeriodStart),
		DatabaseBranchPublicId: ii.DatabaseBranchPublicID,
		VtgateName:             hostnameOrEmpty(),
		NormalizedSql:          stringOrNil(key.SQL),
		StatementType:          pa.StatementType,
		TablesUsed:             pa.TablesUsed,
		TabletType:             stringOrNil(key.TabletType),
		ActiveKeyspace:         stringOrNil(key.ActiveKeyspace),
		QueryCount:             pa.QueryCount,
		ErrorCount:             pa.ErrorCount,
		SumShardQueries:        pa.SumShardQueries,
		MaxShardQueries:        pa.MaxShardQueries,
		SumRowsRead:            pa.SumRowsRead,
		MaxRowsRead:            pa.MaxRowsRead,
		SumRowsAffected:        pa.SumRowsAffected,
		MaxRowsAffected:        pa.MaxRowsAffected,
		SumRowsReturned:        pa.SumRowsReturned,
		MaxRowsReturned:        pa.MaxRowsReturned,
		SumTotalDuration:       durationOrNil(pa.SumTotalDuration),
		MaxTotalDuration:       durationOrNil(pa.MaxTotalDuration),
		SumPlanDuration:        durationOrNil(pa.SumPlanDuration),
		MaxPlanDuration:        durationOrNil(pa.MaxPlanDuration),
		SumExecuteDuration:     durationOrNil(pa.SumExecuteDuration),
		MaxExecuteDuration:     durationOrNil(pa.MaxExecuteDuration),
		SumCommitDuration:      durationOrNil(pa.SumCommitDuration),
		MaxCommitDuration:      durationOrNil(pa.MaxCommitDuration),
		BoostQueryPublicId:     stringOrNil(key.BoostQueryPublicID),
	}

	if ii.SendTotalDurationSketches {
		obj.TotalDurationSketch = &pbvtgate.QueryStatsBundle_DDSketch{
			Gamma:   float32(ii.TotalDurationSketchGamma),
			Sum:     float64(pa.SumTotalDuration) * ii.TotalDurationSketchUnitsMultiplier,
			Units:   int32(ii.TotalDurationSketchUnits),
			Count:   uint32(pa.QueryCount),
			Buckets: pa.TotalDurationSketchBuckets,
		}
	}

	var out []byte
	var err error
	if ii.KafkaText {
		out, err = prototext.Marshal(&obj)
	} else {
		out, err = obj.MarshalVT()
	}
	if err != nil {
		return nil, err
	}
	return ii.makeEnvelope(out, queryStatsBundleTopic)
}

func (ii *Insights) makeEnvelope(contents []byte, topic string) ([]byte, error) {
	envelope := pbenvelope.Envelope{
		TypeUrl:   queryURLBase + "/" + topic,
		Event:     contents,
		Id:        uuid.NewString(),
		Timestamp: timestamppb.Now(),
	}

	if ii.KafkaText {
		return prototext.Marshal(&envelope)
	}
	return envelope.MarshalVT()
}

func (ii *Insights) normalizeSQL(stmt sqlparser.Statement, maybeReorderColumns bool) (string, *uint32) {
	// We normalize queries that differ only by the orders of the columns in INSERT statements, but only for
	// customers where we detect that's a problem.  We detect it's a problem by counting the number of
	// query patterns that differ only in column order.  To do that, we calculate what the hash would be if
	// we ignored the column list, then count the number of distinct patterns that have that same hash.
	// Once it exceeds ReorderColumnThreshold, we set ReorderInsertColumns=true for this vtgate for as long
	// as it runs.
	//
	// skipSpans is all the substrings (left and right byte offsets) of buf.String() that constitute column lists
	// and should be skipped by the hash function.
	type span struct {
		left, right int
	}
	var skipSpans []span

	buf := sqlparser.NewTrackedBuffer(func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case sqlparser.Columns:
			if !maybeReorderColumns || len(node) == 0 {
				node.Format(buf)
			} else if ii.ReorderInsertColumns {
				sort.Slice(node, func(i, j int) bool {
					return node[i].Lowered() < node[j].Lowered()
				})
				node.Format(buf)
			} else {
				left := buf.Len()
				node.Format(buf)
				right := buf.Len()
				skipSpans = append(skipSpans, span{left, right})
			}
		case *sqlparser.ComparisonExpr:
			if node.Operator == sqlparser.InOp {
				switch node.Right.(type) {
				case *sqlparser.Subquery: // "IN <subquery>" is unmodified
					node.Format(buf)
				default:
					buf.Myprintf("%l in (<elements>)", node.Left)
				}
			} else if node.Operator == sqlparser.NotInOp {
				switch node.Right.(type) {
				case *sqlparser.Subquery: // "IN <subquery>" is unmodified
					node.Format(buf)
				default:
					buf.Myprintf("%l not in (<elements>)", node.Left)
				}
			} else {
				node.Format(buf)
			}
		case sqlparser.Values:
			buf.WriteString("values <values>")
		case *sqlparser.Savepoint:
			buf.WriteString("savepoint <id>")
		case *sqlparser.Release:
			buf.WriteString("release savepoint <id>")
		case *sqlparser.UnaryExpr:
			if normalizesToPlaceholder(node.Expr) {
				buf.WriteRune('?')
			} else {
				node.Format(buf)
			}
		case *sqlparser.IntroducerExpr:
			if normalizesToPlaceholder(node.Expr) {
				buf.WriteRune('?')
			} else {
				node.Format(buf)
			}
		case *sqlparser.ParsedComments:
			// elide comments entirely
		default:
			if normalizesToPlaceholder(node) {
				buf.WriteRune('?')
			} else {
				node.Format(buf)
			}
		}
	})

	ret := buf.WriteNode(stmt).String()
	if skipSpans != nil {
		var hash uint32
		prev := 0
		for _, sp := range skipSpans {
			hash = murmur3.SeedStringSum32(hash, ret[prev:sp.left])
			prev = sp.right
		}
		hash = murmur3.SeedStringSum32(hash, ret[prev:])
		return ret, &hash
	}
	return ret, nil
}

// true for any node type that would generate a '?' placeholder
func normalizesToPlaceholder(node sqlparser.SQLNode) bool {
	switch node.(type) {
	case *sqlparser.Argument, sqlparser.BoolVal, *sqlparser.NullVal, *sqlparser.Literal:
		return true
	}
	return false
}

// First capture group in pattern is replaced with `replacment`
var normalizations = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`elapsed time: ([^\s,]+)`), `<time>`},
	{regexp.MustCompile(`query ID ([\d]+)`), `<id>`},
	{regexp.MustCompile(`transaction ([\d]+)`), `<transaction>`},
	{regexp.MustCompile(`ended at ([^\s]+ [^\s]+ [^\s]+)`), `<time>`},
	{regexp.MustCompile(`conn ([\d]+)`), `<conn>`},
	{regexp.MustCompile(`The table ('[^\s]+')`), `<table>`},
	{regexp.MustCompile(`at row ([\d]+)`), `<row>`},
	{regexp.MustCompile(`at position ([\d]+)`), `<position>`},
	{regexp.MustCompile(`( \(CallerID: [^\s]+\))$`), ``},
}

// Remove highly variable components of error messages (i.e. query ids, dates, etc) so that errors can be grouped
// together when shown to the user.
func normalizeError(error string) string {
	for _, n := range normalizations {
		if idx := n.pattern.FindStringSubmatchIndex(error); len(idx) >= 4 {
			error = error[0:idx[2]] + n.replacement + error[idx[3]:]
		}
	}
	return error
}

func stringOrNil(s string) *wrapperspb.StringValue {
	if s == "" {
		return nil
	}
	return wrapperspb.String(s)
}

func durationOrNil(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}

func (ii *Insights) MockTimer() {
	// Send a nil to the LogChan to force a flush.  Only for use in unit tests.
	ii.LogChan <- nil
}
