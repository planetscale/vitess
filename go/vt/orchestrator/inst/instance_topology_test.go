package inst

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

var (
	i710Key = InstanceKey{Hostname: "i710", Port: 3306}
	i720Key = InstanceKey{Hostname: "i720", Port: 3306}
	i730Key = InstanceKey{Hostname: "i730", Port: 3306}
	i810Key = InstanceKey{Hostname: "i810", Port: 3306}
	i820Key = InstanceKey{Hostname: "i820", Port: 3306}
	i830Key = InstanceKey{Hostname: "i830", Port: 3306}
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func generateTestInstances() (instances [](*Instance), instancesMap map[string](*Instance)) {
	i710 := Instance{Hostname: i710Key.Hostname, Port: i710Key.Port, ServerID: 710, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 10}}
	i720 := Instance{Hostname: i720Key.Hostname, Port: i720Key.Port, ServerID: 720, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 20}}
	i730 := Instance{Hostname: i730Key.Hostname, Port: i730Key.Port, ServerID: 730, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 30}}
	i810 := Instance{Hostname: i810Key.Hostname, Port: i810Key.Port, ServerID: 810, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000008", LogPos: 10}}
	i820 := Instance{Hostname: i820Key.Hostname, Port: i820Key.Port, ServerID: 820, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000008", LogPos: 20}}
	i830 := Instance{Hostname: i830Key.Hostname, Port: i830Key.Port, ServerID: 830, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000008", LogPos: 30}}
	instances = [](*Instance){&i710, &i720, &i730, &i810, &i820, &i830}
	for _, instance := range instances {
		instance.Version = "5.6.7"
		instance.BinlogFormat = "STATEMENT"
	}
	instancesMap = make(map[string](*Instance))
	for _, instance := range instances {
		instancesMap[instance.Key().StringCode()] = instance
	}
	return instances, instancesMap
}

func applyGeneralGoodToGoReplicationParams(instances [](*Instance)) {
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = true
	}
}

func TestInitial(t *testing.T) {
	require.True(t, true)
}

func TestSortInstances(t *testing.T) {
	instances, _ := generateTestInstances()
	sortInstances(instances)
	require.EqualValues(t, instances[0].Key().String(), i830Key.String())
	require.EqualValues(t, instances[1].Key().String(), i820Key.String())
	require.EqualValues(t, instances[2].Key().String(), i810Key.String())
	require.EqualValues(t, instances[3].Key().String(), i730Key.String())
	require.EqualValues(t, instances[4].Key().String(), i720Key.String())
	require.EqualValues(t, instances[5].Key().String(), i710Key.String())
}

func TestSortInstancesSameCoordinatesDifferingBinlogFormats(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
		instance.BinlogFormat = "MIXED"
	}
	instancesMap[i810Key.StringCode()].BinlogFormat = "STATEMENT"
	instancesMap[i720Key.StringCode()].BinlogFormat = "ROW"
	sortInstances(instances)
	require.EqualValues(t, instances[0].Key().String(), i810Key.String())
	require.EqualValues(t, instances[5].Key().String(), i720Key.String())
}

func TestSortInstancesSameCoordinatesDifferingVersions(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
	}
	instancesMap[i810Key.StringCode()].Version = "5.5.1"
	instancesMap[i720Key.StringCode()].Version = "5.7.8"
	sortInstances(instances)
	require.EqualValues(t, instances[0].Key().String(), i810Key.String())
	require.EqualValues(t, instances[5].Key().String(), i720Key.String())
}

func TestSortInstancesDataCenterHint(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
		instance.DataCenter = "somedc"
	}
	instancesMap[i810Key.StringCode()].DataCenter = "localdc"
	SortInstancesDataCenterHint(instances, "localdc")
	require.EqualValues(t, instances[0].Key().String(), i810Key.String())
}

func TestSortInstancesGtidErrant(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
		instance.GtidErrant = "00020192-1111-1111-1111-111111111111:1"
	}
	instancesMap[i810Key.StringCode()].GtidErrant = ""
	sortInstances(instances)
	require.EqualValues(t, instances[0].Key().String(), i810Key.String())
}

func TestGetPriorityMajorVersionForCandidate(t *testing.T) {
	{
		instances, instancesMap := generateTestInstances()

		priorityMajorVersion, err := getPriorityMajorVersionForCandidate(instances)
		require.NoError(t, err)
		require.EqualValues(t, priorityMajorVersion, "5.6")

		instancesMap[i810Key.StringCode()].Version = "5.5.1"
		instancesMap[i720Key.StringCode()].Version = "5.7.8"
		priorityMajorVersion, err = getPriorityMajorVersionForCandidate(instances)
		require.NoError(t, err)
		require.EqualValues(t, priorityMajorVersion, "5.6")

		instancesMap[i710Key.StringCode()].Version = "5.7.8"
		instancesMap[i720Key.StringCode()].Version = "5.7.8"
		instancesMap[i730Key.StringCode()].Version = "5.7.8"
		instancesMap[i830Key.StringCode()].Version = "5.7.8"
		priorityMajorVersion, err = getPriorityMajorVersionForCandidate(instances)
		require.NoError(t, err)
		require.EqualValues(t, priorityMajorVersion, "5.7")
	}
	{
		instances, instancesMap := generateTestInstances()

		instancesMap[i710Key.StringCode()].Version = "5.6.9"
		instancesMap[i720Key.StringCode()].Version = "5.6.9"
		instancesMap[i730Key.StringCode()].Version = "5.7.8"
		instancesMap[i810Key.StringCode()].Version = "5.7.8"
		instancesMap[i820Key.StringCode()].Version = "5.7.8"
		instancesMap[i830Key.StringCode()].Version = "5.6.9"
		priorityMajorVersion, err := getPriorityMajorVersionForCandidate(instances)
		require.NoError(t, err)
		require.EqualValues(t, priorityMajorVersion, "5.6")
	}
	// We will be testing under conditions that map iteration is in random order.
	for range rand.Perm(20) { // Just running many iterations to cover multiple possible map iteration ordering. Perm() is just used as an array generator here.
		instances, _ := generateTestInstances()
		for _, instance := range instances {
			instance.Version = "5.6.9"
		}
		require.EqualValues(t, len(instances), 6)
		// Randomly populating different elements of the array/map
		perm := rand.Perm(len(instances))[0 : len(instances)/2]
		for _, i := range perm {
			instances[i].Version = "5.7.8"
		}
		// getPriorityMajorVersionForCandidate uses map iteration
		priorityMajorVersion, err := getPriorityMajorVersionForCandidate(instances)
		require.NoError(t, err)
		require.EqualValues(t, priorityMajorVersion, "5.6")
	}
}

func TestGetPriorityBinlogFormatForCandidate(t *testing.T) {
	{
		instances, instancesMap := generateTestInstances()

		priorityBinlogFormat, err := getPriorityBinlogFormatForCandidate(instances)
		require.NoError(t, err)
		require.EqualValues(t, priorityBinlogFormat, "STATEMENT")

		instancesMap[i810Key.StringCode()].BinlogFormat = "MIXED"
		instancesMap[i720Key.StringCode()].BinlogFormat = "ROW"
		priorityBinlogFormat, err = getPriorityBinlogFormatForCandidate(instances)
		require.NoError(t, err)
		require.EqualValues(t, priorityBinlogFormat, "STATEMENT")

		instancesMap[i710Key.StringCode()].BinlogFormat = "ROW"
		instancesMap[i720Key.StringCode()].BinlogFormat = "ROW"
		instancesMap[i730Key.StringCode()].BinlogFormat = "ROW"
		instancesMap[i830Key.StringCode()].BinlogFormat = "ROW"
		priorityBinlogFormat, err = getPriorityBinlogFormatForCandidate(instances)
		require.NoError(t, err)
		require.EqualValues(t, priorityBinlogFormat, "ROW")
	}
	for _, lowBinlogFormat := range []string{"STATEMENT", "MIXED"} {
		// We will be testing under conditions that map iteration is in random order.
		for range rand.Perm(20) { // Just running many iterations to cover multiple possible map iteration ordering. Perm() is just used as an array generator here.
			instances, _ := generateTestInstances()
			for _, instance := range instances {
				instance.BinlogFormat = lowBinlogFormat
			}
			require.EqualValues(t, len(instances), 6)
			// Randomly populating different elements of the array/map
			perm := rand.Perm(len(instances))[0 : len(instances)/2]
			for _, i := range perm {
				instances[i].BinlogFormat = "ROW"
			}
			// getPriorityBinlogFormatForCandidate uses map iteration
			priorityBinlogFormat, err := getPriorityBinlogFormatForCandidate(instances)
			require.NoError(t, err)
			require.EqualValues(t, priorityBinlogFormat, lowBinlogFormat)
		}
	}
}

func TestIsGenerallyValidAsBinlogSource(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		require.False(t, isGenerallyValidAsBinlogSource(instance))
	}
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		require.True(t, isGenerallyValidAsBinlogSource(instance))
	}
}

func TestIsGenerallyValidAsCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		require.False(t, isGenerallyValidAsCandidateReplica(instance))
	}
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = false
	}
	for _, instance := range instances {
		require.False(t, isGenerallyValidAsCandidateReplica(instance))
	}
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		require.True(t, isGenerallyValidAsCandidateReplica(instance))
	}
}

func TestIsBannedFromBeingCandidateReplica(t *testing.T) {
	{
		instances, _ := generateTestInstances()
		for _, instance := range instances {
			require.False(t, IsBannedFromBeingCandidateReplica(instance))
		}
	}
	{
		instances, _ := generateTestInstances()
		for _, instance := range instances {
			instance.PromotionRule = promotionrule.MustNot
		}
		for _, instance := range instances {
			require.True(t, IsBannedFromBeingCandidateReplica(instance))
		}
	}
	{
		instances, _ := generateTestInstances()
		config.Config.PromotionIgnoreHostnameFilters = []string{
			"i7",
			"i8[0-9]0",
		}
		for _, instance := range instances {
			require.True(t, IsBannedFromBeingCandidateReplica(instance))
		}
		config.Config.PromotionIgnoreHostnameFilters = []string{}
	}
}

func TestChooseCandidateReplicaNoCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = false
	}
	_, _, _, _, _, err := ChooseCandidateReplica(instances)
	require.NotNil(t, err)
}

func TestChooseCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i830Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 5)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplica2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].LogReplicationUpdatesEnabled = false
	instancesMap[i820Key.StringCode()].LogBinEnabled = false
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i810Key.String())
	require.EqualValues(t, len(aheadReplicas), 2)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 3)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaSameCoordinatesDifferentVersions(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
	}
	instancesMap[i810Key.StringCode()].Version = "5.5.1"
	instancesMap[i720Key.StringCode()].Version = "5.7.8"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i810Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 5)
	require.EqualValues(t, len(laterReplicas), 0)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionNoLoss(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.5.1"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i830Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 5)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionLosesOne(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i820Key.String())
	require.EqualValues(t, len(aheadReplicas), 1)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 4)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionLosesTwo(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instancesMap[i820Key.StringCode()].Version = "5.7.18"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i810Key.String())
	require.EqualValues(t, len(aheadReplicas), 2)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 3)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionHigherVersionOverrides(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instancesMap[i820Key.StringCode()].Version = "5.7.18"
	instancesMap[i810Key.StringCode()].Version = "5.7.5"
	instancesMap[i730Key.StringCode()].Version = "5.7.30"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i830Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 3)
	require.EqualValues(t, len(cannotReplicateReplicas), 2)
}

func TestChooseCandidateReplicaLosesOneDueToBinlogFormat(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.BinlogFormat = "ROW"
	}
	instancesMap[i730Key.StringCode()].BinlogFormat = "STATEMENT"

	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i830Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 4)
	require.EqualValues(t, len(cannotReplicateReplicas), 1)
}

func TestChooseCandidateReplicaPriorityBinlogFormatNoLoss(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.BinlogFormat = "MIXED"
	}
	instancesMap[i830Key.StringCode()].BinlogFormat = "STATEMENT"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i830Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 5)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatLosesOne(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].BinlogFormat = "ROW"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i820Key.String())
	require.EqualValues(t, len(aheadReplicas), 1)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 4)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatLosesTwo(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].BinlogFormat = "ROW"
	instancesMap[i820Key.StringCode()].BinlogFormat = "ROW"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i810Key.String())
	require.EqualValues(t, len(aheadReplicas), 2)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 3)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatRowOverrides(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].BinlogFormat = "ROW"
	instancesMap[i820Key.StringCode()].BinlogFormat = "ROW"
	instancesMap[i810Key.StringCode()].BinlogFormat = "ROW"
	instancesMap[i730Key.StringCode()].BinlogFormat = "ROW"
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i830Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 3)
	require.EqualValues(t, len(cannotReplicateReplicas), 2)
}

func TestChooseCandidateReplicaMustNotPromoteRule(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].PromotionRule = promotionrule.MustNot
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i820Key.String())
	require.EqualValues(t, len(aheadReplicas), 1)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 4)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPreferNotPromoteRule(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].PromotionRule = promotionrule.MustNot
	instancesMap[i820Key.StringCode()].PromotionRule = promotionrule.PreferNot
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i820Key.String())
	require.EqualValues(t, len(aheadReplicas), 1)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 4)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPreferNotPromoteRule2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.PromotionRule = promotionrule.PreferNot
	}
	instancesMap[i830Key.StringCode()].PromotionRule = promotionrule.MustNot
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i820Key.String())
	require.EqualValues(t, len(aheadReplicas), 1)
	require.EqualValues(t, len(equalReplicas), 0)
	require.EqualValues(t, len(laterReplicas), 4)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = promotionrule.Neutral
	}
	instancesMap[i830Key.StringCode()].PromotionRule = promotionrule.Prefer
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i830Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 5)
	require.EqualValues(t, len(laterReplicas), 0)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = promotionrule.Prefer
	}
	instancesMap[i820Key.StringCode()].PromotionRule = promotionrule.Must
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i820Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 5)
	require.EqualValues(t, len(laterReplicas), 0)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering3(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = promotionrule.Neutral
	}
	instancesMap[i730Key.StringCode()].PromotionRule = promotionrule.Must
	instancesMap[i810Key.StringCode()].PromotionRule = promotionrule.Prefer
	instancesMap[i830Key.StringCode()].PromotionRule = promotionrule.PreferNot
	instances = sortedReplicas(instances, NoStopReplication)
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ChooseCandidateReplica(instances)
	require.NoError(t, err)
	require.EqualValues(t, candidate.Key().String(), i730Key.String())
	require.EqualValues(t, len(aheadReplicas), 0)
	require.EqualValues(t, len(equalReplicas), 5)
	require.EqualValues(t, len(laterReplicas), 0)
	require.EqualValues(t, len(cannotReplicateReplicas), 0)
}
