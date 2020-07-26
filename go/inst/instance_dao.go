/*
   Copyright 2014 Outbrain Inc.

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

package inst

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/math"
	"github.com/outbrain/golib/sqlutils"
	"github.com/outbrain/orchestrator/go/attributes"
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/db"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
)

const backendDBConcurrency = 20

var instanceReadChan = make(chan bool, backendDBConcurrency)
var instanceWriteChan = make(chan bool, backendDBConcurrency)

// InstancesByCountSubordinateHosts is a sortable type for Instance
type InstancesByCountSubordinateHosts [](*Instance)

func (this InstancesByCountSubordinateHosts) Len() int      { return len(this) }
func (this InstancesByCountSubordinateHosts) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountSubordinateHosts) Less(i, j int) bool {
	return len(this[i].SubordinateHosts) < len(this[j].SubordinateHosts)
}

// instanceKeyInformativeClusterName is a non-authoritative cache; used for auditing or general purpose.
var instanceKeyInformativeClusterName *cache.Cache

var readTopologyInstanceCounter = metrics.NewCounter()
var readInstanceCounter = metrics.NewCounter()
var writeInstanceCounter = metrics.NewCounter()

func init() {
	metrics.Register("instance.read_topology", readTopologyInstanceCounter)
	metrics.Register("instance.read", readInstanceCounter)
	metrics.Register("instance.write", writeInstanceCounter)
}

func InitializeInstanceDao() {
	instanceWriteBuffer = make(chan instanceUpdateObject, config.Config.InstanceWriteBufferSize)
	instanceKeyInformativeClusterName = cache.New(time.Duration(config.Config.InstancePollSeconds/2)*time.Second, time.Second)

	// spin off instance write buffer flushing
	go func() {
		flushTick := time.Tick(time.Duration(config.Config.InstanceFlushIntervalMilliseconds) * time.Millisecond)
		for {
			// it is time to flush
			select {
			case <-flushTick:
				flushInstanceWriteBuffer()
			case <-forceFlushInstanceWriteBuffer:
				flushInstanceWriteBuffer()
			}
		}
	}()
}

// ExecDBWriteFunc chooses how to execute a write onto the database: whether synchronuously or not
func ExecDBWriteFunc(f func() error) error {
	instanceWriteChan <- true
	defer func() { recover(); <-instanceWriteChan }()
	res := f()
	return res
}

// logReadTopologyInstanceError logs an error, if applicable, for a ReadTopologyInstance operation,
// providing context and hint as for the source of the error.
func logReadTopologyInstanceError(instanceKey *InstanceKey, hint string, err error) error {
	if err == nil {
		return nil
	}
	return log.Errorf("ReadTopologyInstance(%+v) %+v: %+v", *instanceKey, hint, err)
}

func ReadTopologyInstanceUnbuffered(instanceKey *InstanceKey) (*Instance, error) {
	return ReadTopologyInstance(instanceKey, false)
}

// ReadTopologyInstance connects to a topology MySQL instance and reads its configuration and
// replication status. It writes read info into orchestrator's backend.
// Writes are optionally buffered.
func ReadTopologyInstance(instanceKey *InstanceKey, bufferWrites bool) (*Instance, error) {
	defer func() {
		if err := recover(); err != nil {
			logReadTopologyInstanceError(instanceKey, "Unexpected, aborting", fmt.Errorf("%+v", err))
		}
	}()

	instance := NewInstance()
	instanceFound := false
	foundByShowSubordinateHosts := false
	longRunningProcesses := []Process{}
	resolvedHostname := ""
	maxScaleMainHostname := ""
	isMaxScale := false
	isMaxScale110 := false
	subordinateStatusFound := false
	var resolveErr error

	if !instanceKey.IsValid() {
		_ = UpdateInstanceLastAttemptedCheck(instanceKey)
		return instance, fmt.Errorf("ReadTopologyInstance will not act on invalid instance key: %+v", *instanceKey)
	}

	db, err := db.OpenDiscovery(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		goto Cleanup
	}

	instance.Key = *instanceKey

	{
		// Is this MaxScale? (a proxy, not a real server)
		err = sqlutils.QueryRowsMap(db, "show variables like 'maxscale%'", func(m sqlutils.RowMap) error {
			variableName := m.GetString("Variable_name")
			if variableName == "MAXSCALE_VERSION" {
				originalVersion := m.GetString("Value")
				if originalVersion == "" {
					originalVersion = m.GetString("value")
				}
				if originalVersion == "" {
					originalVersion = "0.0.0"
				}
				instance.Version = originalVersion + "-maxscale"
				instance.ServerID = 0
				instance.ServerUUID = ""
				instance.Uptime = 0
				instance.Binlog_format = "INHERIT"
				instance.ReadOnly = true
				instance.LogBinEnabled = true
				instance.LogSubordinateUpdatesEnabled = true
				resolvedHostname = instance.Key.Hostname
				UpdateResolvedHostname(resolvedHostname, resolvedHostname)
				isMaxScale = true
			}
			return nil
		})
		if err != nil {
			logReadTopologyInstanceError(instanceKey, "show variables like 'maxscale%'", err)
			// We do not "goto Cleanup" here, although it should be the correct flow.
			// Reason is 5.7's new security feature that requires GRANTs on performance_schema.session_variables.
			// There is a wrong decision making in this design and the migration path to 5.7 will be difficult.
			// I don't want orchestrator to put even more burden on this.
			// If the statement errors, then we are unable to determine that this is maxscale, hence assume it is not.
			// In which case there would be other queries sent to the server that are not affected by 5.7 behavior, and that will fail.
		}
	}

	if isMaxScale && strings.Contains(instance.Version, "1.1.0") {
		isMaxScale110 = true
	}
	if isMaxScale110 {
		// Buggy buggy maxscale 1.1.0. Reported Main_Host can be corrupted.
		// Therefore we (currently) take @@hostname (which is masquarading as main host anyhow)
		err = db.QueryRow("select @@hostname").Scan(&maxScaleMainHostname)
		if err != nil {
			goto Cleanup
		}
	}
	if isMaxScale {
		if isMaxScale110 {
			// Only this is supported:
			db.QueryRow("select @@server_id").Scan(&instance.ServerID)
		} else {
			db.QueryRow("select @@global.server_id").Scan(&instance.ServerID)
			db.QueryRow("select @@global.server_uuid").Scan(&instance.ServerUUID)
		}
	}

	if !isMaxScale {
		var mysqlHostname, mysqlReportHost string
		err = db.QueryRow("select @@global.hostname, ifnull(@@global.report_host, ''), @@global.server_id, @@global.version, @@global.read_only, @@global.binlog_format, @@global.log_bin, @@global.log_subordinate_updates").Scan(
			&mysqlHostname, &mysqlReportHost, &instance.ServerID, &instance.Version, &instance.ReadOnly, &instance.Binlog_format, &instance.LogBinEnabled, &instance.LogSubordinateUpdatesEnabled)
		if err != nil {
			goto Cleanup
		}
		switch strings.ToLower(config.Config.MySQLHostnameResolveMethod) {
		case "none":
			resolvedHostname = instance.Key.Hostname
		case "default", "hostname", "@@hostname":
			resolvedHostname = mysqlHostname
		case "report_host", "@@report_host":
			if mysqlReportHost == "" {
				err = fmt.Errorf("MySQLHostnameResolveMethod configured to use @@report_host but %+v has NULL/empty @@report_host", instanceKey)
				goto Cleanup
			}
			resolvedHostname = mysqlReportHost
		default:
			resolvedHostname = instance.Key.Hostname
		}

		if instance.IsOracleMySQL() && !instance.IsSmallerMajorVersionByString("5.6") {
			var mainInfoRepositoryOnTable bool
			// Stuff only supported on Oracle MySQL >= 5.6
			// ...
			// @@gtid_mode only available in Orcale MySQL >= 5.6
			// Previous version just issued this query brute-force, but I don't like errors being issued where they shouldn't.
			_ = db.QueryRow("select @@global.gtid_mode = 'ON', @@global.server_uuid, @@global.gtid_purged, @@global.main_info_repository = 'TABLE'").Scan(&instance.SupportsOracleGTID, &instance.ServerUUID, &instance.GtidPurged, &mainInfoRepositoryOnTable)
			if mainInfoRepositoryOnTable {
				_ = db.QueryRow("select count(*) > 0 and MAX(User_name) != '' from mysql.subordinate_main_info").Scan(&instance.ReplicationCredentialsAvailable)
			}
		}
	}
	{
		var dummy string
		// show global status works just as well with 5.6 & 5.7 (5.7 moves variables to performance_schema)
		err = db.QueryRow("show global status like 'Uptime'").Scan(&dummy, &instance.Uptime)

		if err != nil {
			logReadTopologyInstanceError(instanceKey, "show global status like 'Uptime'", err)

			// We do not "goto Cleanup" here, although it should be the correct flow.
			// Reason is 5.7's new security feature that requires GRANTs on performance_schema.global_variables.
			// There is a wrong decisionmaking in this design and the migration path to 5.7 will be difficult.
			// I don't want orchestrator to put even more burden on this. The 'Uptime' variable is not that important
			// so as to completely fail reading a 5.7 instance.
			// This is supposed to be fixed in 5.7.9
		}
	}
	if resolvedHostname != instance.Key.Hostname {
		UpdateResolvedHostname(instance.Key.Hostname, resolvedHostname)
		instance.Key.Hostname = resolvedHostname
	}
	if instance.Key.Hostname == "" {
		err = fmt.Errorf("ReadTopologyInstance: empty hostname (%+v). Bailing out", *instanceKey)
		goto Cleanup
	}
	if config.Config.DataCenterPattern != "" {
		if pattern, err := regexp.Compile(config.Config.DataCenterPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.DataCenter = match[1]
			}
		}
		// This can be overriden by later invocation of DetectDataCenterQuery
	}
	if config.Config.PhysicalEnvironmentPattern != "" {
		if pattern, err := regexp.Compile(config.Config.PhysicalEnvironmentPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.PhysicalEnvironment = match[1]
			}
		}
		// This can be overriden by later invocation of DetectPhysicalEnvironmentQuery
	}

	err = sqlutils.QueryRowsMap(db, "show subordinate status", func(m sqlutils.RowMap) error {
		instance.HasReplicationCredentials = (m.GetString("Main_User") != "")
		instance.Subordinate_IO_Running = (m.GetString("Subordinate_IO_Running") == "Yes")
		if isMaxScale110 {
			// Covering buggy MaxScale 1.1.0
			instance.Subordinate_IO_Running = instance.Subordinate_IO_Running && (m.GetString("Subordinate_IO_State") == "Binlog Dump")
		}
		instance.Subordinate_SQL_Running = (m.GetString("Subordinate_SQL_Running") == "Yes")
		instance.ReadBinlogCoordinates.LogFile = m.GetString("Main_Log_File")
		instance.ReadBinlogCoordinates.LogPos = m.GetInt64("Read_Main_Log_Pos")
		instance.ExecBinlogCoordinates.LogFile = m.GetString("Relay_Main_Log_File")
		instance.ExecBinlogCoordinates.LogPos = m.GetInt64("Exec_Main_Log_Pos")
		instance.IsDetached, _, _ = instance.ExecBinlogCoordinates.DetachedCoordinates()
		instance.RelaylogCoordinates.LogFile = m.GetString("Relay_Log_File")
		instance.RelaylogCoordinates.LogPos = m.GetInt64("Relay_Log_Pos")
		instance.RelaylogCoordinates.Type = RelayLog
		instance.LastSQLError = strconv.QuoteToASCII(m.GetString("Last_SQL_Error"))
		instance.LastIOError = strconv.QuoteToASCII(m.GetString("Last_IO_Error"))
		instance.SQLDelay = m.GetUintD("SQL_Delay", 0)
		instance.UsingOracleGTID = (m.GetIntD("Auto_Position", 0) == 1)
		instance.ExecutedGtidSet = m.GetStringD("Executed_Gtid_Set", "")
		instance.UsingMariaDBGTID = (m.GetStringD("Using_Gtid", "No") != "No")
		instance.HasReplicationFilters = ((m.GetStringD("Replicate_Do_DB", "") != "") || (m.GetStringD("Replicate_Ignore_DB", "") != "") || (m.GetStringD("Replicate_Do_Table", "") != "") || (m.GetStringD("Replicate_Ignore_Table", "") != "") || (m.GetStringD("Replicate_Wild_Do_Table", "") != "") || (m.GetStringD("Replicate_Wild_Ignore_Table", "") != ""))

		mainHostname := m.GetString("Main_Host")
		if isMaxScale110 {
			// Buggy buggy maxscale 1.1.0. Reported Main_Host can be corrupted.
			// Therefore we (currently) take @@hostname (which is masquarading as main host anyhow)
			mainHostname = maxScaleMainHostname
		}
		mainKey, err := NewInstanceKeyFromStrings(mainHostname, m.GetString("Main_Port"))
		if err != nil {
			logReadTopologyInstanceError(instanceKey, "NewInstanceKeyFromStrings", err)
		}
		mainKey.Hostname, resolveErr = ResolveHostname(mainKey.Hostname)
		if resolveErr != nil {
			logReadTopologyInstanceError(instanceKey, fmt.Sprintf("ResolveHostname(%q)", mainKey.Hostname), resolveErr)
		}
		instance.MainKey = *mainKey
		instance.IsDetachedMain = instance.MainKey.IsDetached()
		instance.SecondsBehindMain = m.GetNullInt64("Seconds_Behind_Main")
		if instance.SecondsBehindMain.Valid && instance.SecondsBehindMain.Int64 < 0 {
			log.Warningf("Host: %+v, instance.SecondsBehindMain < 0 [%+v], correcting to 0", instanceKey, instance.SecondsBehindMain.Int64)
			instance.SecondsBehindMain.Int64 = 0
		}
		// And until told otherwise:
		instance.SubordinateLagSeconds = instance.SecondsBehindMain

		instance.AllowTLS = (m.GetString("Main_SSL_Allowed") == "Yes")
		// Not breaking the flow even on error
		subordinateStatusFound = true
		return nil
	})
	if err != nil {
		goto Cleanup
	}
	if isMaxScale && !subordinateStatusFound {
		err = fmt.Errorf("No 'SHOW SLAVE STATUS' output found for a MaxScale instance: %+v", instanceKey)
		goto Cleanup
	}

	if instance.LogBinEnabled {
		err = sqlutils.QueryRowsMap(db, "show main status", func(m sqlutils.RowMap) error {
			var err error
			instance.SelfBinlogCoordinates.LogFile = m.GetString("File")
			instance.SelfBinlogCoordinates.LogPos = m.GetInt64("Position")
			return err
		})
		if err != nil {
			goto Cleanup
		}
	}

	instanceFound = true

	// -------------------------------------------------------------------------
	// Anything after this point does not affect the fact the instance is found.
	// No `goto Cleanup` after this point.
	// -------------------------------------------------------------------------

	// Get subordinates, either by SHOW SLAVE HOSTS or via PROCESSLIST
	// MaxScale does not support PROCESSLIST, so SHOW SLAVE HOSTS is the only option
	if config.Config.DiscoverByShowSubordinateHosts || isMaxScale {
		err := sqlutils.QueryRowsMap(db, `show subordinate hosts`,
			func(m sqlutils.RowMap) error {
				// MaxScale 1.1 may trigger an error with this command, but
				// also we may see issues if anything on the MySQL server locks up.
				// Consequently it's important to validate the values received look
				// good prior to calling ResolveHostname()
				host := m.GetString("Host")
				port := m.GetString("Port")
				if host == "" || port == "" {
					if isMaxScale110 && host == "" && port == "0" {
						// MaxScale 1.1.0 reports a bad response sometimes so ignore it.
						return nil
					}
					// otherwise report the error to the caller
					return fmt.Errorf("ReadTopologyInstance(%+v) 'show subordinate hosts' returned row with <host,port>: <%v,%v>", instanceKey, host, port)
				}
				// Note: NewInstanceKeyFromStrings calls ResolveHostname() implicitly
				subordinateKey, err := NewInstanceKeyFromStrings(host, port)
				if err == nil && subordinateKey.IsValid() {
					instance.AddSubordinateKey(subordinateKey)
					foundByShowSubordinateHosts = true
				}
				return err
			})

		logReadTopologyInstanceError(instanceKey, "show subordinate hosts", err)
	}
	if !foundByShowSubordinateHosts && !isMaxScale {
		// Either not configured to read SHOW SLAVE HOSTS or nothing was there.
		// Discover by processlist
		err := sqlutils.QueryRowsMap(db, `
        	select
        		substring_index(host, ':', 1) as subordinate_hostname
        	from
        		information_schema.processlist
        	where
        		command='Binlog Dump'
        		or command='Binlog Dump GTID'
        		`,
			func(m sqlutils.RowMap) error {
				cname, resolveErr := ResolveHostname(m.GetString("subordinate_hostname"))
				if resolveErr != nil {
					logReadTopologyInstanceError(instanceKey, "ResolveHostname: processlist", resolveErr)
				}
				subordinateKey := InstanceKey{Hostname: cname, Port: instance.Key.Port}
				instance.AddSubordinateKey(&subordinateKey)
				return err
			})

		logReadTopologyInstanceError(instanceKey, "processlist", err)
	}

	if config.Config.ReadLongRunningQueries && !isMaxScale {
		// Get long running processes
		err := sqlutils.QueryRowsMap(db, `
				  select
				    id,
				    user,
				    host,
				    db,
				    command,
				    time,
				    state,
				    left(processlist.info, 1024) as info,
				    now() - interval time second as started_at
				  from
				    information_schema.processlist
				  where
				    time > 60
				    and command != 'Sleep'
				    and id != connection_id()
				    and user != 'system user'
				    and command != 'Binlog dump'
				    and command != 'Binlog Dump GTID'
				    and user != 'event_scheduler'
				  order by
				    time desc
        		`,
			func(m sqlutils.RowMap) error {
				process := Process{}
				process.Id = m.GetInt64("id")
				process.User = m.GetString("user")
				process.Host = m.GetString("host")
				process.Db = m.GetString("db")
				process.Command = m.GetString("command")
				process.Time = m.GetInt64("time")
				process.State = m.GetString("state")
				process.Info = m.GetString("info")
				process.StartedAt = m.GetString("started_at")

				longRunningProcesses = append(longRunningProcesses, process)
				return nil
			})

		logReadTopologyInstanceError(instanceKey, "processlist, long queries", err)
	}

	instance.UsingPseudoGTID = false
	if config.Config.DetectPseudoGTIDQuery != "" && !isMaxScale {
		if resultData, err := sqlutils.QueryResultData(db, config.Config.DetectPseudoGTIDQuery); err == nil {
			if len(resultData) > 0 {
				if len(resultData[0]) > 0 {
					if resultData[0][0].Valid && resultData[0][0].String == "1" {
						instance.UsingPseudoGTID = true
					}
				}
			}
		} else {
			logReadTopologyInstanceError(instanceKey, "DetectPseudoGTIDQuery", err)
		}
	}

	if config.Config.SubordinateLagQuery != "" && !isMaxScale {
		if err := db.QueryRow(config.Config.SubordinateLagQuery).Scan(&instance.SubordinateLagSeconds); err == nil {
			if instance.SubordinateLagSeconds.Valid && instance.SubordinateLagSeconds.Int64 < 0 {
				log.Warningf("Host: %+v, instance.SubordinateLagSeconds < 0 [%+v], correcting to 0", instanceKey, instance.SubordinateLagSeconds.Int64)
				instance.SubordinateLagSeconds.Int64 = 0
			}
		} else {
			instance.SubordinateLagSeconds = instance.SecondsBehindMain
			logReadTopologyInstanceError(instanceKey, "SubordinateLagQuery", err)
		}
	}

	if config.Config.DetectDataCenterQuery != "" && !isMaxScale {
		err := db.QueryRow(config.Config.DetectDataCenterQuery).Scan(&instance.DataCenter)
		logReadTopologyInstanceError(instanceKey, "DetectDataCenterQuery", err)
	}

	if config.Config.DetectPhysicalEnvironmentQuery != "" && !isMaxScale {
		err := db.QueryRow(config.Config.DetectPhysicalEnvironmentQuery).Scan(&instance.PhysicalEnvironment)
		logReadTopologyInstanceError(instanceKey, "DetectPhysicalEnvironmentQuery", err)
	}

	if config.Config.DetectInstanceAliasQuery != "" && !isMaxScale {
		err := db.QueryRow(config.Config.DetectInstanceAliasQuery).Scan(&instance.InstanceAlias)
		logReadTopologyInstanceError(instanceKey, "DetectInstanceAliasQuery", err)
	}

	if config.Config.DetectSemiSyncEnforcedQuery != "" && !isMaxScale {
		err := db.QueryRow(config.Config.DetectSemiSyncEnforcedQuery).Scan(&instance.SemiSyncEnforced)
		logReadTopologyInstanceError(instanceKey, "DetectSemiSyncEnforcedQuery", err)
	}

	{
		err = ReadInstanceClusterAttributes(instance)
		logReadTopologyInstanceError(instanceKey, "ReadInstanceClusterAttributes", err)
	}

	// First read the current PromotionRule from candidate_database_instance.
	{
		err = ReadInstancePromotionRule(instance)
		logReadTopologyInstanceError(instanceKey, "ReadInstancePromotionRule", err)
	}
	// Then check if the instance wants to set a different PromotionRule.
	// We'll set it here on their behalf so there's no race between the first
	// time an instance is discovered, and setting a rule like "must_not".
	if config.Config.DetectPromotionRuleQuery != "" && !isMaxScale {
		var value string
		err := db.QueryRow(config.Config.DetectPromotionRuleQuery).Scan(&value)
		logReadTopologyInstanceError(instanceKey, "DetectPromotionRuleQuery", err)
		promotionRule, err := ParseCandidatePromotionRule(value)
		logReadTopologyInstanceError(instanceKey, "ParseCandidatePromotionRule", err)
		if err == nil {
			// We need to update candidate_database_instance.
			// We register the rule even if it hasn't changed,
			// to bump the last_suggested time.
			instance.PromotionRule = promotionRule
			err = RegisterCandidateInstance(instanceKey, promotionRule)
			logReadTopologyInstanceError(instanceKey, "RegisterCandidateInstance", err)
		}
	}

	if instance.ReplicationDepth == 0 && config.Config.DetectClusterAliasQuery != "" && !isMaxScale {
		// Only need to do on mains
		clusterAlias := ""
		err := db.QueryRow(config.Config.DetectClusterAliasQuery).Scan(&clusterAlias)
		if err != nil {
			clusterAlias = ""
			logReadTopologyInstanceError(instanceKey, "DetectClusterAliasQuery", err)
		}
		instance.SuggestedClusterAlias = clusterAlias
	}
	if instance.ReplicationDepth == 0 && config.Config.DetectClusterDomainQuery != "" && !isMaxScale {
		// Only need to do on mains
		domainName := ""
		if err := db.QueryRow(config.Config.DetectClusterDomainQuery).Scan(&domainName); err != nil {
			domainName = ""
			logReadTopologyInstanceError(instanceKey, "DetectClusterDomainQuery", err)
		}
		if domainName != "" {
			err := WriteClusterDomainName(instance.ClusterName, domainName)
			logReadTopologyInstanceError(instanceKey, "WriteClusterDomainName", err)
		}
	}

Cleanup:
	readTopologyInstanceCounter.Inc(1)
	logReadTopologyInstanceError(instanceKey, "Cleanup", err)
	if instanceFound {
		instance.IsLastCheckValid = true
		instance.IsRecentlyChecked = true
		instance.IsUpToDate = true
		if bufferWrites {
			enqueueInstanceWrite(instance, instanceFound, err)
		} else {
			writeInstance(instance, instanceFound, err)
		}
		WriteLongRunningProcesses(&instance.Key, longRunningProcesses)
		return instance, nil
	}

	// Something is wrong, could be network-wise. Record that we
	// tried to check the instance. last_attempted_check is also
	// updated on success by writeInstance.
	_ = UpdateInstanceLastAttemptedCheck(instanceKey)
	_ = UpdateInstanceLastChecked(&instance.Key)
	return nil, fmt.Errorf("Failed ReadTopologyInstance")
}

// ReadInstanceClusterAttributes will return the cluster name for a given instance by looking at its main
// and getting it from there.
// It is a non-recursive function and so-called-recursion is performed upon periodic reading of
// instances.
func ReadInstanceClusterAttributes(instance *Instance) (err error) {
	if config.Config.DatabaselessMode__experimental {
		return nil
	}

	var mainMainKey InstanceKey
	var mainClusterName string
	var mainReplicationDepth uint
	mainDataFound := false

	// Read the cluster_name of the _main_ of our instance, derive it from there.
	query := `
			select
					cluster_name,
					replication_depth,
					main_host,
					main_port
				from database_instance
				where hostname=? and port=?
	`
	args := sqlutils.Args(instance.MainKey.Hostname, instance.MainKey.Port)

	err = db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		mainClusterName = m.GetString("cluster_name")
		mainReplicationDepth = m.GetUint("replication_depth")
		mainMainKey.Hostname = m.GetString("main_host")
		mainMainKey.Port = m.GetInt("main_port")
		mainDataFound = true
		return nil
	})
	if err != nil {
		return log.Errore(err)
	}

	var replicationDepth uint = 0
	var clusterName string
	if mainDataFound {
		replicationDepth = mainReplicationDepth + 1
		clusterName = mainClusterName
	}
	clusterNameByInstanceKey := instance.Key.StringCode()
	if clusterName == "" {
		// Nothing from main; we set it to be named after the instance itself
		clusterName = clusterNameByInstanceKey
	}

	isCoMain := false
	if mainMainKey.Equals(&instance.Key) {
		// co-main calls for special case, in fear of the infinite loop
		isCoMain = true
		clusterNameByCoMainKey := instance.MainKey.StringCode()
		if clusterName != clusterNameByInstanceKey && clusterName != clusterNameByCoMainKey {
			// Can be caused by a co-main topology failover
			log.Errorf("ReadInstanceClusterAttributes: in co-main topology %s is not in (%s, %s). Forcing it to become one of them", clusterName, clusterNameByInstanceKey, clusterNameByCoMainKey)
			clusterName = math.TernaryString(instance.Key.SmallerThan(&instance.MainKey), clusterNameByInstanceKey, clusterNameByCoMainKey)
		}
		if clusterName == clusterNameByInstanceKey {
			// circular replication. Avoid infinite ++ on replicationDepth
			replicationDepth = 0
		} // While the other stays "1"
	}
	instance.ClusterName = clusterName
	instance.ReplicationDepth = replicationDepth
	instance.IsCoMain = isCoMain
	return nil
}

// BulkReadInstance returns a list of all instances from the database
// - hostname:port is good enough
func BulkReadInstance() ([](*InstanceKey), error) {
	var instances [](*InstanceKey)

	// table scan - I know.
	query := `
SELECT	hostname, port
FROM	database_instance
`

	err := db.QueryOrchestrator(query, nil, func(m sqlutils.RowMap) error {
		instanceKey := &InstanceKey{
			Hostname: m.GetString("hostname"),
			Port:     m.GetInt("port"),
		}
		instances = append(instances, instanceKey)

		log.Debugf("BulkReadInstance: %+v", instanceKey)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return instances, nil
}

func ReadInstancePromotionRule(instance *Instance) (err error) {
	if config.Config.DatabaselessMode__experimental {
		return nil
	}

	var promotionRule CandidatePromotionRule = NeutralPromoteRule
	// Read the cluster_name of the _main_ of our instance, derive it from there.
	query := `
			select
					ifnull(nullif(promotion_rule, ''), 'neutral') as promotion_rule
				from candidate_database_instance
				where hostname=? and port=?
	`
	args := sqlutils.Args(instance.Key.Hostname, instance.Key.Port)

	err = db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		promotionRule = CandidatePromotionRule(m.GetString("promotion_rule"))
		return nil
	})
	instance.PromotionRule = promotionRule
	return log.Errore(err)
}

// readInstanceRow reads a single instance row from the orchestrator backend database.
func readInstanceRow(m sqlutils.RowMap) *Instance {
	instance := NewInstance()

	instance.Key.Hostname = m.GetString("hostname")
	instance.Key.Port = m.GetInt("port")
	instance.Uptime = m.GetUint("uptime")
	instance.ServerID = m.GetUint("server_id")
	instance.ServerUUID = m.GetString("server_uuid")
	instance.Version = m.GetString("version")
	instance.ReadOnly = m.GetBool("read_only")
	instance.Binlog_format = m.GetString("binlog_format")
	instance.LogBinEnabled = m.GetBool("log_bin")
	instance.LogSubordinateUpdatesEnabled = m.GetBool("log_subordinate_updates")
	instance.MainKey.Hostname = m.GetString("main_host")
	instance.MainKey.Port = m.GetInt("main_port")
	instance.IsDetachedMain = instance.MainKey.IsDetached()
	instance.Subordinate_SQL_Running = m.GetBool("subordinate_sql_running")
	instance.Subordinate_IO_Running = m.GetBool("subordinate_io_running")
	instance.HasReplicationFilters = m.GetBool("has_replication_filters")
	instance.SupportsOracleGTID = m.GetBool("supports_oracle_gtid")
	instance.UsingOracleGTID = m.GetBool("oracle_gtid")
	instance.ExecutedGtidSet = m.GetString("executed_gtid_set")
	instance.GtidPurged = m.GetString("gtid_purged")
	instance.UsingMariaDBGTID = m.GetBool("mariadb_gtid")
	instance.UsingPseudoGTID = m.GetBool("pseudo_gtid")
	instance.SelfBinlogCoordinates.LogFile = m.GetString("binary_log_file")
	instance.SelfBinlogCoordinates.LogPos = m.GetInt64("binary_log_pos")
	instance.ReadBinlogCoordinates.LogFile = m.GetString("main_log_file")
	instance.ReadBinlogCoordinates.LogPos = m.GetInt64("read_main_log_pos")
	instance.ExecBinlogCoordinates.LogFile = m.GetString("relay_main_log_file")
	instance.ExecBinlogCoordinates.LogPos = m.GetInt64("exec_main_log_pos")
	instance.IsDetached, _, _ = instance.ExecBinlogCoordinates.DetachedCoordinates()
	instance.RelaylogCoordinates.LogFile = m.GetString("relay_log_file")
	instance.RelaylogCoordinates.LogPos = m.GetInt64("relay_log_pos")
	instance.RelaylogCoordinates.Type = RelayLog
	instance.LastSQLError = m.GetString("last_sql_error")
	instance.LastIOError = m.GetString("last_io_error")
	instance.SecondsBehindMain = m.GetNullInt64("seconds_behind_main")
	instance.SubordinateLagSeconds = m.GetNullInt64("subordinate_lag_seconds")
	instance.SQLDelay = m.GetUint("sql_delay")
	subordinateHostsJSON := m.GetString("subordinate_hosts")
	instance.ClusterName = m.GetString("cluster_name")
	instance.SuggestedClusterAlias = m.GetString("suggested_cluster_alias")
	instance.DataCenter = m.GetString("data_center")
	instance.PhysicalEnvironment = m.GetString("physical_environment")
	instance.SemiSyncEnforced = m.GetBool("semi_sync_enforced")
	instance.ReplicationDepth = m.GetUint("replication_depth")
	instance.IsCoMain = m.GetBool("is_co_main")
	instance.ReplicationCredentialsAvailable = m.GetBool("replication_credentials_available")
	instance.HasReplicationCredentials = m.GetBool("has_replication_credentials")
	instance.IsUpToDate = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds)
	instance.IsRecentlyChecked = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds*5)
	instance.LastSeenTimestamp = m.GetString("last_seen")
	instance.IsLastCheckValid = m.GetBool("is_last_check_valid")
	instance.SecondsSinceLastSeen = m.GetNullInt64("seconds_since_last_seen")
	instance.IsCandidate = m.GetBool("is_candidate")
	instance.PromotionRule = CandidatePromotionRule(m.GetString("promotion_rule"))
	instance.IsDowntimed = m.GetBool("is_downtimed")
	instance.DowntimeReason = m.GetString("downtime_reason")
	instance.DowntimeOwner = m.GetString("downtime_owner")
	instance.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
	instance.UnresolvedHostname = m.GetString("unresolved_hostname")
	instance.AllowTLS = m.GetBool("allow_tls")
	instance.InstanceAlias = m.GetString("instance_alias")

	instance.SubordinateHosts.ReadJson(subordinateHostsJSON)
	return instance
}

// readInstancesByCondition is a generic function to read instances from the backend database
func readInstancesByCondition(condition string, args []interface{}, sort string) ([](*Instance), error) {
	readFunc := func() ([](*Instance), error) {
		instances := [](*Instance){}

		if sort == "" {
			sort = `hostname, port`
		}
		query := fmt.Sprintf(`
		select
			*,
			timestampdiff(second, last_checked, now()) as seconds_since_last_checked,
			(last_checked <= last_seen) is true as is_last_check_valid,
			timestampdiff(second, last_seen, now()) as seconds_since_last_seen,
			candidate_database_instance.last_suggested is not null
				 and candidate_database_instance.promotion_rule in ('must', 'prefer') as is_candidate,
			ifnull(nullif(candidate_database_instance.promotion_rule, ''), 'neutral') as promotion_rule,
			ifnull(unresolved_hostname, '') as unresolved_hostname,
			(
    		database_instance_downtime.downtime_active IS NULL
    		or database_instance_downtime.end_timestamp < NOW()
    	) is false as is_downtimed,
    	ifnull(database_instance_downtime.reason, '') as downtime_reason,
    	ifnull(database_instance_downtime.owner, '') as downtime_owner,
    	ifnull(database_instance_downtime.end_timestamp, '') as downtime_end_timestamp
		from
			database_instance
			left join candidate_database_instance using (hostname, port)
			left join hostname_unresolve using (hostname)
			left join database_instance_downtime using (hostname, port)
		where
			%s
		order by
			%s
			`, condition, sort)

		err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
			instance := readInstanceRow(m)
			instances = append(instances, instance)
			return nil
		})
		if err != nil {
			return instances, log.Errore(err)
		}
		err = PopulateInstancesAgents(instances)
		if err != nil {
			return instances, log.Errore(err)
		}
		return instances, err
	}
	instanceReadChan <- true
	instances, err := readFunc()
	<-instanceReadChan
	return instances, err
}

// ReadInstance reads an instance from the orchestrator backend database
func ReadInstance(instanceKey *InstanceKey) (*Instance, bool, error) {
	if config.Config.DatabaselessMode__experimental {
		instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
		return instance, (err == nil), err
	}

	condition := `
			hostname = ?
			and port = ?
		`
	instances, err := readInstancesByCondition(condition, sqlutils.Args(instanceKey.Hostname, instanceKey.Port), "")
	// We know there will be at most one (hostname & port are PK)
	// And we expect to find one
	readInstanceCounter.Inc(1)
	if len(instances) == 0 {
		return nil, false, err
	}
	if err != nil {
		return instances[0], false, err
	}
	return instances[0], true, nil
}

// ReadClusterInstances reads all instances of a given cluster
func ReadClusterInstances(clusterName string) ([](*Instance), error) {
	if strings.Index(clusterName, "'") >= 0 {
		return [](*Instance){}, log.Errorf("Invalid cluster name: %s", clusterName)
	}
	condition := `cluster_name = ?`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
}

// ReadClusterWriteableMain returns the/a writeable main of this cluster
// Typically, the cluster name indicates the main of the cluster. However, in circular
// main-main replication one main can assume the name of the cluster, and it is
// not guaranteed that it is the writeable one.
func ReadClusterWriteableMain(clusterName string) ([](*Instance), error) {
	condition := `
		cluster_name = ?
		and read_only = 0
		and (replication_depth = 0 or is_co_main)
	`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "replication_depth asc")
}

// ReadWriteableClustersMains returns writeable mains of all clusters, but only one
// per cluster, in similar logic to ReadClusterWriteableMain
func ReadWriteableClustersMains() (instances [](*Instance), err error) {
	condition := `
		read_only = 0
		and (replication_depth = 0 or is_co_main)
	`
	allMains, err := readInstancesByCondition(condition, sqlutils.Args(), "cluster_name asc, replication_depth asc")
	if err != nil {
		return instances, err
	}
	visitedClusters := make(map[string]bool)
	for _, instance := range allMains {
		if !visitedClusters[instance.ClusterName] {
			visitedClusters[instance.ClusterName] = true
			instances = append(instances, instance)
		}
	}
	return instances, err
}

// ReadSubordinateInstances reads subordinates of a given main
func ReadSubordinateInstances(mainKey *InstanceKey) ([](*Instance), error) {
	condition := `
			main_host = ?
			and main_port = ?
		`
	return readInstancesByCondition(condition, sqlutils.Args(mainKey.Hostname, mainKey.Port), "")
}

// ReadSubordinateInstancesIncludingBinlogServerSubSubordinates returns a list of direct slves including any subordinates
// of a binlog server replica
func ReadSubordinateInstancesIncludingBinlogServerSubSubordinates(mainKey *InstanceKey) ([](*Instance), error) {
	subordinates, err := ReadSubordinateInstances(mainKey)
	if err != nil {
		return subordinates, err
	}
	for _, subordinate := range subordinates {
		subordinate := subordinate
		if subordinate.IsBinlogServer() {
			binlogServerSubordinates, err := ReadSubordinateInstancesIncludingBinlogServerSubSubordinates(&subordinate.Key)
			if err != nil {
				return subordinates, err
			}
			subordinates = append(subordinates, binlogServerSubordinates...)
		}
	}
	return subordinates, err
}

// ReadBinlogServerSubordinateInstances reads direct subordinates of a given main that are binlog servers
func ReadBinlogServerSubordinateInstances(mainKey *InstanceKey) ([](*Instance), error) {
	condition := `
			main_host = ?
			and main_port = ?
			and binlog_server = 1
		`
	return readInstancesByCondition(condition, sqlutils.Args(mainKey.Hostname, mainKey.Port), "")
}

// ReadUnseenInstances reads all instances which were not recently seen
func ReadUnseenInstances() ([](*Instance), error) {
	condition := `last_seen < last_checked`
	return readInstancesByCondition(condition, sqlutils.Args(), "")
}

// ReadProblemInstances reads all instances with problems
func ReadProblemInstances(clusterName string) ([](*Instance), error) {
	condition := `
			cluster_name LIKE IF(? = '', '%', ?)
			and (
				(last_seen < last_checked)
				or (not ifnull(timestampdiff(second, last_checked, now()) <= ?, false))
				or (not subordinate_sql_running)
				or (not subordinate_io_running)
				or (abs(cast(seconds_behind_main as signed) - cast(sql_delay as signed)) > ?)
				or (abs(cast(subordinate_lag_seconds as signed) - cast(sql_delay as signed)) > ?)
			)
		`

	args := sqlutils.Args(clusterName, clusterName, config.Config.InstancePollSeconds, config.Config.ReasonableReplicationLagSeconds, config.Config.ReasonableReplicationLagSeconds)
	instances, err := readInstancesByCondition(condition, args, "")
	if err != nil {
		return instances, err
	}
	var reportedInstances [](*Instance)
	for _, instance := range instances {
		skip := false
		if instance.IsDowntimed {
			skip = true
		}
		for _, filter := range config.Config.ProblemIgnoreHostnameFilters {
			if matched, _ := regexp.MatchString(filter, instance.Key.Hostname); matched {
				skip = true
			}
		}
		if !skip {
			reportedInstances = append(reportedInstances, instance)
		}
	}
	return reportedInstances, nil
}

// SearchInstances reads all instances qualifying for some searchString
func SearchInstances(searchString string) ([](*Instance), error) {
	searchString = strings.TrimSpace(searchString)
	condition := `
			locate(?, hostname) > 0
			or locate(?, cluster_name) > 0
			or locate(?, version) > 0
			or locate(?, concat(hostname, ':', port)) > 0
			or concat(server_id, '') = ?
			or concat(port, '') = ?
		`
	args := sqlutils.Args(searchString, searchString, searchString, searchString, searchString, searchString)
	return readInstancesByCondition(condition, args, `replication_depth asc, num_subordinate_hosts desc, cluster_name, hostname, port`)
}

// FindInstances reads all instances whose name matches given pattern
func FindInstances(regexpPattern string) ([](*Instance), error) {
	condition := `hostname rlike ?`
	return readInstancesByCondition(condition, sqlutils.Args(regexpPattern), `replication_depth asc, num_subordinate_hosts desc, cluster_name, hostname, port`)
}

// FindFuzzyInstances return instances whose names are like the one given (host & port substrings)
// For example, the given `mydb-3:3306` might find `myhosts-mydb301-production.mycompany.com:3306`
func FindFuzzyInstances(fuzzyInstanceKey *InstanceKey) ([](*Instance), error) {
	condition := `
		hostname like concat('%', ?, '%')
		and port = ?
	`
	return readInstancesByCondition(condition, sqlutils.Args(fuzzyInstanceKey.Hostname, fuzzyInstanceKey.Port), `replication_depth asc, num_subordinate_hosts desc, cluster_name, hostname, port`)
}

// FindClusterNameByFuzzyInstanceKey attempts to find a uniquely identifyable cluster name
// given a fuzze key. It hopes to find instances matching given fuzzy key such that they all
// belong to same cluster
func FindClusterNameByFuzzyInstanceKey(fuzzyInstanceKey *InstanceKey) (string, error) {
	clusterNames := make(map[string]bool)
	instances, err := FindFuzzyInstances(fuzzyInstanceKey)
	if err != nil {
		return "", err
	}
	for _, instance := range instances {
		clusterNames[instance.ClusterName] = true
	}
	if len(clusterNames) == 1 {
		for clusterName := range clusterNames {
			return clusterName, nil
		}
	}
	return "", log.Errorf("findClusterNameByFuzzyInstanceKey: cannot uniquely identify cluster name by %+v", *fuzzyInstanceKey)
}

// ReadFuzzyInstanceKey accepts a fuzzy instance key and expects to return a single, fully qualified,
// known instance key.
func ReadFuzzyInstanceKey(fuzzyInstanceKey *InstanceKey) *InstanceKey {
	if fuzzyInstanceKey == nil {
		return nil
	}
	if fuzzyInstanceKey.Hostname != "" {
		// Fuzzy instance search
		if fuzzyInstances, _ := FindFuzzyInstances(fuzzyInstanceKey); len(fuzzyInstances) == 1 {
			return &(fuzzyInstances[0].Key)
		}
	}
	return nil
}

// ReadFuzzyInstanceKeyIfPossible accepts a fuzzy instance key and hopes to return a single, fully qualified,
// known instance key, or else the original given key
func ReadFuzzyInstanceKeyIfPossible(fuzzyInstanceKey *InstanceKey) *InstanceKey {
	if instanceKey := ReadFuzzyInstanceKey(fuzzyInstanceKey); instanceKey != nil {
		return instanceKey
	}
	return fuzzyInstanceKey
}

// ReadFuzzyInstance accepts a fuzzy instance key and expects to return a single instance.
// Multiple instances matching the fuzzy keys are not allowed.
func ReadFuzzyInstance(fuzzyInstanceKey *InstanceKey) (*Instance, error) {
	if fuzzyInstanceKey == nil {
		return nil, log.Errorf("ReadFuzzyInstance received nil input")
	}
	if fuzzyInstanceKey.Hostname != "" {
		// Fuzzy instance search
		if fuzzyInstances, _ := FindFuzzyInstances(fuzzyInstanceKey); len(fuzzyInstances) == 1 {
			return fuzzyInstances[0], nil
		}
	}
	return nil, log.Errorf("Cannot determine fuzzy instance %+v", *fuzzyInstanceKey)
}

// ReadLostInRecoveryInstances returns all instances (potentially filtered by cluster)
// which are currently indicated as downtimed due to being lost during a topology recovery.
// Keep in mind:
// - instances are only marked as such when config's MainFailoverLostInstancesDowntimeMinutes > 0
// - The downtime expires at some point
func ReadLostInRecoveryInstances(clusterName string) ([](*Instance), error) {
	condition := `
		ifnull(
			database_instance_downtime.downtime_active = 1
			and database_instance_downtime.end_timestamp > now()
			and database_instance_downtime.reason = ?, false)
		and ? IN ('', cluster_name)
	`
	return readInstancesByCondition(condition, sqlutils.Args(DowntimeLostInRecoveryMessage, clusterName), "cluster_name asc, replication_depth asc")
}

// ReadClusterCandidateInstances reads cluster instances which are also marked as candidates
func ReadClusterCandidateInstances(clusterName string) ([](*Instance), error) {
	condition := `
			cluster_name = ?
			and (hostname, port) in (
				select hostname, port
					from candidate_database_instance
					where promotion_rule in ('must', 'prefer')
			)
			`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
}

// filterOSCInstances will filter the given list such that only subordinates fit for OSC control remain.
func filterOSCInstances(instances [](*Instance)) [](*Instance) {
	result := [](*Instance){}
	for _, instance := range instances {
		skipThisHost := false
		for _, filter := range config.Config.OSCIgnoreHostnameFilters {
			if matched, _ := regexp.MatchString(filter, instance.Key.Hostname); matched {
				skipThisHost = true
			}
		}
		if instance.IsBinlogServer() {
			skipThisHost = true
		}

		if !instance.IsLastCheckValid {
			skipThisHost = true
		}
		if !skipThisHost {
			result = append(result, instance)
		}
	}
	return result
}

// GetClusterOSCSubordinates returns a heuristic list of subordinates which are fit as controll subordinates for an OSC operation.
// These would be intermediate mains
func GetClusterOSCSubordinates(clusterName string) ([](*Instance), error) {
	intermediateMains := [](*Instance){}
	result := [](*Instance){}
	var err error
	if strings.Index(clusterName, "'") >= 0 {
		return [](*Instance){}, log.Errorf("Invalid cluster name: %s", clusterName)
	}
	{
		// Pick up to two busiest IMs
		condition := `
			replication_depth = 1
			and num_subordinate_hosts > 0
			and cluster_name = ?
		`
		intermediateMains, err = readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		sort.Sort(sort.Reverse(InstancesByCountSubordinateHosts(intermediateMains)))
		intermediateMains = filterOSCInstances(intermediateMains)
		intermediateMains = intermediateMains[0:math.MinInt(2, len(intermediateMains))]
		result = append(result, intermediateMains...)
	}
	{
		// Get 2 subordinates of found IMs, if possible
		if len(intermediateMains) == 1 {
			// Pick 2 subordinates for this IM
			subordinates, err := ReadSubordinateInstances(&(intermediateMains[0].Key))
			if err != nil {
				return result, err
			}
			sort.Sort(sort.Reverse(InstancesByCountSubordinateHosts(subordinates)))
			subordinates = filterOSCInstances(subordinates)
			subordinates = subordinates[0:math.MinInt(2, len(subordinates))]
			result = append(result, subordinates...)

		}
		if len(intermediateMains) == 2 {
			// Pick one subordinate from each IM (should be possible)
			for _, im := range intermediateMains {
				subordinates, err := ReadSubordinateInstances(&im.Key)
				if err != nil {
					return result, err
				}
				sort.Sort(sort.Reverse(InstancesByCountSubordinateHosts(subordinates)))
				subordinates = filterOSCInstances(subordinates)
				if len(subordinates) > 0 {
					result = append(result, subordinates[0])
				}
			}
		}
	}
	{
		// Get 2 3rd tier subordinates, if possible
		condition := `
			replication_depth = 3
			and cluster_name = ?
		`
		subordinates, err := readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		sort.Sort(sort.Reverse(InstancesByCountSubordinateHosts(subordinates)))
		subordinates = filterOSCInstances(subordinates)
		subordinates = subordinates[0:math.MinInt(2, len(subordinates))]
		result = append(result, subordinates...)
	}
	{
		// Get 2 1st tier leaf subordinates, if possible
		condition := `
			replication_depth = 1
			and num_subordinate_hosts = 0
			and cluster_name = ?
		`
		subordinates, err := readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		subordinates = filterOSCInstances(subordinates)
		subordinates = subordinates[0:math.MinInt(2, len(subordinates))]
		result = append(result, subordinates...)
	}

	return result, nil
}

// GetClusterGhostSubordinates returns a list of replicas that can serve as the connected servers
// for a [gh-ost](https://github.com/github/gh-ost) operation. A gh-ost operation prefers to talk
// to a RBR replica that has no children.
func GetClusterGhostSubordinates(clusterName string) (result [](*Instance), err error) {
	condition := `
			replication_depth > 0
			and binlog_format = 'ROW'
			and cluster_name = ?
		`
	instances, err := readInstancesByCondition(condition, sqlutils.Args(clusterName), "num_subordinate_hosts asc")
	if err != nil {
		return result, err
	}

	for _, instance := range instances {
		skipThisHost := false
		if instance.IsBinlogServer() {
			skipThisHost = true
		}
		if !instance.IsLastCheckValid {
			skipThisHost = true
		}
		if !instance.LogBinEnabled {
			skipThisHost = true
		}
		if !instance.LogSubordinateUpdatesEnabled {
			skipThisHost = true
		}
		if !skipThisHost {
			result = append(result, instance)
		}
	}

	return result, err
}

// GetInstancesMaxLag returns the maximum lag in a set of instances
func GetInstancesMaxLag(instances [](*Instance)) (maxLag int64, err error) {
	if len(instances) == 0 {
		return 0, log.Errorf("No instances found in GetInstancesMaxLag")
	}
	for _, clusterInstance := range instances {
		if clusterInstance.SubordinateLagSeconds.Valid && clusterInstance.SubordinateLagSeconds.Int64 > maxLag {
			maxLag = clusterInstance.SubordinateLagSeconds.Int64
		}
	}
	return maxLag, nil
}

// GetClusterHeuristicLag returns a heuristic lag for a cluster, based on its OSC subordinates
func GetClusterHeuristicLag(clusterName string) (int64, error) {
	instances, err := GetClusterOSCSubordinates(clusterName)
	if err != nil {
		return 0, err
	}
	return GetInstancesMaxLag(instances)
}

// GetHeuristicClusterPoolInstances returns instances of a cluster which are also pooled. If `pool` argument
// is empty, all pools are considered, otherwise, only instances of given pool are considered.
func GetHeuristicClusterPoolInstances(clusterName string, pool string) (result [](*Instance), err error) {
	instances, err := ReadClusterInstances(clusterName)
	if err != nil {
		return result, err
	}

	pooledInstanceKeys := NewInstanceKeyMap()
	clusterPoolInstances, err := ReadClusterPoolInstances(clusterName, pool)
	if err != nil {
		return result, err
	}
	for _, clusterPoolInstance := range clusterPoolInstances {
		pooledInstanceKeys.AddKey(InstanceKey{Hostname: clusterPoolInstance.Hostname, Port: clusterPoolInstance.Port})
	}

	for _, instance := range instances {
		skipThisHost := false
		if instance.IsBinlogServer() {
			skipThisHost = true
		}
		if !instance.IsLastCheckValid {
			skipThisHost = true
		}
		if !pooledInstanceKeys.HasKey(instance.Key) {
			skipThisHost = true
		}
		if !skipThisHost {
			result = append(result, instance)
		}
	}

	return result, err
}

// GetHeuristicClusterPoolInstancesLag returns a heuristic lag for the instances participating
// in a cluster pool (or all the cluster's pools)
func GetHeuristicClusterPoolInstancesLag(clusterName string, pool string) (int64, error) {
	instances, err := GetHeuristicClusterPoolInstances(clusterName, pool)
	if err != nil {
		return 0, err
	}
	return GetInstancesMaxLag(instances)
}

// updateInstanceClusterName
func updateInstanceClusterName(instance *Instance) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
			update
				database_instance
			set
				cluster_name=?
			where
				hostname=? and port=?
        	`, instance.ClusterName, instance.Key.Hostname, instance.Key.Port,
		)
		if err != nil {
			return log.Errore(err)
		}
		AuditOperation("update-cluster-name", &instance.Key, fmt.Sprintf("set to %s", instance.ClusterName))
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ReviewUnseenInstances reviews instances that have not been seen (suposedly dead) and updates some of their data
func ReviewUnseenInstances() error {
	instances, err := ReadUnseenInstances()
	if err != nil {
		return log.Errore(err)
	}
	operations := 0
	for _, instance := range instances {
		instance := instance

		mainHostname, err := ResolveHostname(instance.MainKey.Hostname)
		if err != nil {
			log.Errore(err)
			continue
		}
		instance.MainKey.Hostname = mainHostname
		savedClusterName := instance.ClusterName

		if err := ReadInstanceClusterAttributes(instance); err != nil {
			log.Errore(err)
		} else if instance.ClusterName != savedClusterName {
			updateInstanceClusterName(instance)
			operations++
		}
	}

	AuditOperation("review-unseen-instances", nil, fmt.Sprintf("Operations: %d", operations))
	return err
}

// readUnseenMainKeys will read list of mains that have never been seen, and yet whose subordinates
// seem to be replicating.
func readUnseenMainKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}

	err := db.QueryOrchestratorRowsMap(`
			SELECT DISTINCT
			    subordinate_instance.main_host, subordinate_instance.main_port
			FROM
			    database_instance subordinate_instance
			        LEFT JOIN
			    hostname_resolve ON (subordinate_instance.main_host = hostname_resolve.hostname)
			        LEFT JOIN
			    database_instance main_instance ON (
			    	COALESCE(hostname_resolve.resolved_hostname, subordinate_instance.main_host) = main_instance.hostname
			    	and subordinate_instance.main_port = main_instance.port)
			WHERE
			    main_instance.last_checked IS NULL
			    and subordinate_instance.main_host != ''
			    and subordinate_instance.main_host != '_'
			    and subordinate_instance.main_port > 0
			    and subordinate_instance.subordinate_io_running = 1
			`, func(m sqlutils.RowMap) error {
		instanceKey, _ := NewInstanceKeyFromStrings(m.GetString("main_host"), m.GetString("main_port"))
		// we ignore the error. It can be expected that we are unable to resolve the hostname.
		// Maybe that's how we got here in the first place!
		res = append(res, *instanceKey)

		return nil
	})
	if err != nil {
		return res, log.Errore(err)
	}

	return res, nil
}

// InjectUnseenMains will review mains of instances that are known to be replicating, yet which are not listed
// in database_instance. Since their subordinates are listed as replicating, we can assume that such mains actually do
// exist: we shall therefore inject them with minimal details into the database_instance table.
func InjectUnseenMains() error {

	unseenMainKeys, err := readUnseenMainKeys()
	if err != nil {
		return err
	}

	operations := 0
	for _, mainKey := range unseenMainKeys {
		mainKey := mainKey
		clusterName := mainKey.StringCode()
		// minimal details:
		instance := Instance{Key: mainKey, Version: "Unknown", ClusterName: clusterName}
		if err := writeInstance(&instance, false, nil); err == nil {
			operations++
		}
	}

	AuditOperation("inject-unseen-mains", nil, fmt.Sprintf("Operations: %d", operations))
	return err
}

// ForgetUnseenInstancesDifferentlyResolved will purge instances which are invalid, and whose hostname
// appears on the hostname_resolved table; this means some time in the past their hostname was unresovled, and now
// resovled to a different value; the old hostname is never accessed anymore and the old entry should be removed.
func ForgetUnseenInstancesDifferentlyResolved() error {
	sqlResult, err := db.ExecOrchestrator(`
		DELETE FROM
			database_instance
		USING
		    hostname_resolve
		    JOIN database_instance ON (hostname_resolve.hostname = database_instance.hostname)
		WHERE
		    hostname_resolve.hostname != hostname_resolve.resolved_hostname
		    AND (last_checked <= last_seen) IS NOT TRUE
		`,
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("forget-unseen-differently-resolved", nil, fmt.Sprintf("Forgotten instances: %d", rows))
	return err
}

// readUnknownMainHostnameResolves will figure out the resolved hostnames of main-hosts which cannot be found.
// It uses the hostname_resolve_history table to heuristically guess the correct hostname (based on "this was the
// last time we saw this hostname and it resolves into THAT")
func readUnknownMainHostnameResolves() (map[string]string, error) {
	res := make(map[string]string)
	err := db.QueryOrchestratorRowsMap(`
			SELECT DISTINCT
			    subordinate_instance.main_host, hostname_resolve_history.resolved_hostname
			FROM
			    database_instance subordinate_instance
			LEFT JOIN hostname_resolve ON (subordinate_instance.main_host = hostname_resolve.hostname)
			LEFT JOIN database_instance main_instance ON (
			    COALESCE(hostname_resolve.resolved_hostname, subordinate_instance.main_host) = main_instance.hostname
			    and subordinate_instance.main_port = main_instance.port
			) LEFT JOIN hostname_resolve_history ON (subordinate_instance.main_host = hostname_resolve_history.hostname)
			WHERE
			    main_instance.last_checked IS NULL
			    and subordinate_instance.main_host != ''
			    and subordinate_instance.main_host != '_'
			    and subordinate_instance.main_port > 0
			`, func(m sqlutils.RowMap) error {
		res[m.GetString("main_host")] = m.GetString("resolved_hostname")
		return nil
	})
	if err != nil {
		return res, log.Errore(err)
	}

	return res, nil
}

// ResolveUnknownMainHostnameResolves fixes missing hostname resolves based on hostname_resolve_history
// The use case is subordinates replicating from some unknown-hostname which cannot be otherwise found. This could
// happen due to an expire unresolve together with clearing up of hostname cache.
func ResolveUnknownMainHostnameResolves() error {

	hostnameResolves, err := readUnknownMainHostnameResolves()
	if err != nil {
		return err
	}
	for hostname, resolvedHostname := range hostnameResolves {
		UpdateResolvedHostname(hostname, resolvedHostname)
	}

	AuditOperation("resolve-unknown-mains", nil, fmt.Sprintf("Num resolved hostnames: %d", len(hostnameResolves)))
	return err
}

// ReadCountMySQLSnapshots is a utility method to return registered number of snapshots for a given list of hosts
func ReadCountMySQLSnapshots(hostnames []string) (map[string]int, error) {
	res := make(map[string]int)
	if !config.Config.ServeAgentsHttp {
		return res, nil
	}
	query := fmt.Sprintf(`
		select
			hostname,
			count_mysql_snapshots
		from
			host_agent
		where
			hostname in (%s)
		order by
			hostname
		`, sqlutils.InClauseStringValues(hostnames))

	err := db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		res[m.GetString("hostname")] = m.GetInt("count_mysql_snapshots")
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

// PopulateInstancesAgents will fill in extra data acquired from agents for given instances
// At current this is the number of snapshots.
// This isn't too pretty; it's a push-into-instance-data-that-belongs-to-agent thing.
// Originally the need was to visually present the number of snapshots per host on the web/cluster page, which
// indeed proves to be useful in our experience.
func PopulateInstancesAgents(instances [](*Instance)) error {
	if len(instances) == 0 {
		return nil
	}
	hostnames := []string{}
	for _, instance := range instances {
		hostnames = append(hostnames, instance.Key.Hostname)
	}
	agentsCountMySQLSnapshots, err := ReadCountMySQLSnapshots(hostnames)
	if err != nil {
		return err
	}
	for _, instance := range instances {
		if count, ok := agentsCountMySQLSnapshots[instance.Key.Hostname]; ok {
			instance.CountMySQLSnapshots = count
		}
	}

	return nil
}

func GetClusterName(instanceKey *InstanceKey) (clusterName string, err error) {
	if clusterName, found := instanceKeyInformativeClusterName.Get(instanceKey.StringCode()); found {
		return clusterName.(string), nil
	}
	query := `
		select
			ifnull(max(cluster_name), '') as cluster_name
		from
			database_instance
		where
			hostname = ?
			and port = ?
			`
	err = db.QueryOrchestrator(query, sqlutils.Args(instanceKey.Hostname, instanceKey.Port), func(m sqlutils.RowMap) error {
		clusterName = m.GetString("cluster_name")
		instanceKeyInformativeClusterName.Set(instanceKey.StringCode(), clusterName, cache.DefaultExpiration)
		return nil
	})

	return clusterName, log.Errore(err)
}

// ReadClusters reads names of all known clusters
func ReadClusters() (clusterNames []string, err error) {
	clusters, err := ReadClustersInfo("")
	if err != nil {
		return clusterNames, err
	}
	for _, clusterInfo := range clusters {
		clusterNames = append(clusterNames, clusterInfo.ClusterName)
	}
	return clusterNames, nil
}

// ReadClusterInfo reads some info about a given cluster
func ReadClusterInfo(clusterName string) (*ClusterInfo, error) {
	clusters, err := ReadClustersInfo(clusterName)
	if err != nil {
		return &ClusterInfo{}, err
	}
	if len(clusters) != 1 {
		return &ClusterInfo{}, fmt.Errorf("No cluster info found for %s", clusterName)
	}
	return &(clusters[0]), nil
}

// ReadClustersInfo reads names of all known clusters and some aggregated info
func ReadClustersInfo(clusterName string) ([]ClusterInfo, error) {
	clusters := []ClusterInfo{}

	whereClause := ""
	args := sqlutils.Args()
	if clusterName != "" {
		whereClause = `where cluster_name = ?`
		args = append(args, clusterName)
	}
	query := fmt.Sprintf(`
		select
			cluster_name,
			count(*) as count_instances,
			ifnull(min(alias), cluster_name) as alias,
			ifnull(min(domain_name), '') as domain_name
		from
			database_instance
			left join cluster_alias using (cluster_name)
			left join cluster_domain_name using (cluster_name)
		%s
		group by
			cluster_name`, whereClause)

	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		clusterInfo := ClusterInfo{
			ClusterName:    m.GetString("cluster_name"),
			CountInstances: m.GetUint("count_instances"),
			ClusterAlias:   m.GetString("alias"),
			ClusterDomain:  m.GetString("domain_name"),
		}
		clusterInfo.ApplyClusterAlias()
		clusterInfo.ReadRecoveryInfo()

		clusters = append(clusters, clusterInfo)
		return nil
	})

	return clusters, err
}

// HeuristicallyApplyClusterDomainInstanceAttribute writes down the cluster-domain
// to main-hostname as a general attribute, by reading current topology and **trusting** it to be correct
func HeuristicallyApplyClusterDomainInstanceAttribute(clusterName string) (instanceKey *InstanceKey, err error) {
	clusterInfo, err := ReadClusterInfo(clusterName)
	if err != nil {
		return nil, err
	}

	if clusterInfo.ClusterDomain == "" {
		return nil, fmt.Errorf("Cannot find domain name for cluster %+v", clusterName)
	}

	mains, err := ReadClusterWriteableMain(clusterName)
	if err != nil {
		return nil, err
	}
	if len(mains) != 1 {
		return nil, fmt.Errorf("Found %+v potential main for cluster %+v", len(mains), clusterName)
	}
	instanceKey = &mains[0].Key
	return instanceKey, attributes.SetGeneralAttribute(clusterInfo.ClusterDomain, instanceKey.StringCode())
}

// GetHeuristicClusterDomainInstanceAttribute attempts detecting the cluster domain
// for the given cluster, and return the instance key associated as writer with that domain
func GetHeuristicClusterDomainInstanceAttribute(clusterName string) (instanceKey *InstanceKey, err error) {
	clusterInfo, err := ReadClusterInfo(clusterName)
	if err != nil {
		return nil, err
	}

	if clusterInfo.ClusterDomain == "" {
		return nil, fmt.Errorf("Cannot find domain name for cluster %+v", clusterName)
	}

	writerInstanceName, err := attributes.GetGeneralAttribute(clusterInfo.ClusterDomain)
	if err != nil {
		return nil, err
	}
	return NewRawInstanceKey(writerInstanceName)
}

// ReadOutdatedInstanceKeys reads and returns keys for all instances that are not up to date (i.e.
// pre-configured time has passed since they were last checked)
// But we also check for the case where an attempt at instance checking has been made, that hasn't
// resulted in an actual check! This can happen when TCP/IP connections are hung, in which case the "check"
// never returns. In such case we multiply interval by a factor, so as not to open too many connections on
// the instance.
func ReadOutdatedInstanceKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}
	query := `
		select
			hostname, port
		from
			database_instance
		where
			if (
				last_attempted_check <= last_checked,
				last_checked < now() - interval ? second,
				last_checked < now() - interval (? * 2) second
			)
			`
	args := sqlutils.Args(config.Config.InstancePollSeconds, config.Config.InstancePollSeconds)

	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		instanceKey, merr := NewInstanceKeyFromStrings(m.GetString("hostname"), m.GetString("port"))
		if merr != nil {
			log.Errore(merr)
		} else {
			res = append(res, *instanceKey)
		}
		// We don;t return an error because we want to keep filling the outdated instances list.
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err

}

func mkInsertOdku(table string, columns []string, values []string, nrRows int, insertIgnore bool) (string, error) {
	if len(columns) == 0 {
		return "", errors.New("Column list cannot be empty")
	}
	if nrRows < 1 {
		return "", errors.New("nrRows must be a positive number")
	}
	if len(columns) != len(values) {
		return "", errors.New("number of values must be equal to number of columns")
	}

	var q bytes.Buffer
	var ignore string = ""
	if insertIgnore {
		ignore = "ignore"
	}
	var valRow string = fmt.Sprintf("(%s)", strings.Join(values, ", "))
	var val bytes.Buffer
	val.WriteString(valRow)
	for i := 1; i < nrRows; i++ {
		val.WriteString(",\n                ") // indent VALUES, see below
		val.WriteString(valRow)
	}

	var col string = strings.Join(columns, ", ")
	var odku bytes.Buffer
	odku.WriteString(fmt.Sprintf("%s=VALUES(%s)", columns[0], columns[0]))
	for _, c := range columns[1:] {
		odku.WriteString(", ")
		odku.WriteString(fmt.Sprintf("%s=VALUES(%s)", c, c))
	}

	q.WriteString(fmt.Sprintf(`INSERT %s INTO %s
                (%s)
        VALUES
                %s
        ON DUPLICATE KEY UPDATE
                %s
        `,
		ignore, table, col, val.String(), odku.String()))

	return q.String(), nil
}

func mkInsertOdkuForInstances(instances []*Instance, instanceWasActuallyFound bool, updateLastSeen bool) (string, []interface{}) {
	if len(instances) == 0 {
		return "", nil
	}

	insertIgnore := false
	if !instanceWasActuallyFound {
		insertIgnore = true
	}
	var columns = []string{
		"hostname",
		"port",
		"last_checked",
		"last_attempted_check",
		"uptime",
		"server_id",
		"server_uuid",
		"version",
		"binlog_server",
		"read_only",
		"binlog_format",
		"log_bin",
		"log_subordinate_updates",
		"binary_log_file",
		"binary_log_pos",
		"main_host",
		"main_port",
		"subordinate_sql_running",
		"subordinate_io_running",
		"has_replication_filters",
		"supports_oracle_gtid",
		"oracle_gtid",
		"executed_gtid_set",
		"gtid_purged",
		"mariadb_gtid",
		"pseudo_gtid",
		"main_log_file",
		"read_main_log_pos",
		"relay_main_log_file",
		"exec_main_log_pos",
		"relay_log_file",
		"relay_log_pos",
		"last_sql_error",
		"last_io_error",
		"seconds_behind_main",
		"subordinate_lag_seconds",
		"sql_delay",
		"num_subordinate_hosts",
		"subordinate_hosts",
		"cluster_name",
		"suggested_cluster_alias",
		"data_center",
		"physical_environment",
		"replication_depth",
		"is_co_main",
		"replication_credentials_available",
		"has_replication_credentials",
		"allow_tls",
		"semi_sync_enforced",
		"instance_alias",
	}

	var values []string = make([]string, len(columns), len(columns))
	for i, _ := range columns {
		values[i] = "?"
	}
	values[2] = "NOW()" // last_checked
	values[3] = "NOW()" // last_attempted_check

	if updateLastSeen {
		columns = append(columns, "last_seen")
		values = append(values, "NOW()")
	}

	var args []interface{}
	for _, instance := range instances {
		// number of columns minus 2 as last_checked and last_attempted_check
		// updated with NOW()
		args = append(args, instance.Key.Hostname)
		args = append(args, instance.Key.Port)
		args = append(args, instance.Uptime)
		args = append(args, instance.ServerID)
		args = append(args, instance.ServerUUID)
		args = append(args, instance.Version)
		args = append(args, instance.IsBinlogServer())
		args = append(args, instance.ReadOnly)
		args = append(args, instance.Binlog_format)
		args = append(args, instance.LogBinEnabled)
		args = append(args, instance.LogSubordinateUpdatesEnabled)
		args = append(args, instance.SelfBinlogCoordinates.LogFile)
		args = append(args, instance.SelfBinlogCoordinates.LogPos)
		args = append(args, instance.MainKey.Hostname)
		args = append(args, instance.MainKey.Port)
		args = append(args, instance.Subordinate_SQL_Running)
		args = append(args, instance.Subordinate_IO_Running)
		args = append(args, instance.HasReplicationFilters)
		args = append(args, instance.SupportsOracleGTID)
		args = append(args, instance.UsingOracleGTID)
		args = append(args, instance.ExecutedGtidSet)
		args = append(args, instance.GtidPurged)
		args = append(args, instance.UsingMariaDBGTID)
		args = append(args, instance.UsingPseudoGTID)
		args = append(args, instance.ReadBinlogCoordinates.LogFile)
		args = append(args, instance.ReadBinlogCoordinates.LogPos)
		args = append(args, instance.ExecBinlogCoordinates.LogFile)
		args = append(args, instance.ExecBinlogCoordinates.LogPos)
		args = append(args, instance.RelaylogCoordinates.LogFile)
		args = append(args, instance.RelaylogCoordinates.LogPos)
		args = append(args, instance.LastSQLError)
		args = append(args, instance.LastIOError)
		args = append(args, instance.SecondsBehindMain)
		args = append(args, instance.SubordinateLagSeconds)
		args = append(args, instance.SQLDelay)
		args = append(args, len(instance.SubordinateHosts))
		args = append(args, instance.SubordinateHosts.ToJSONString())
		args = append(args, instance.ClusterName)
		args = append(args, instance.SuggestedClusterAlias)
		args = append(args, instance.DataCenter)
		args = append(args, instance.PhysicalEnvironment)
		args = append(args, instance.ReplicationDepth)
		args = append(args, instance.IsCoMain)
		args = append(args, instance.ReplicationCredentialsAvailable)
		args = append(args, instance.HasReplicationCredentials)
		args = append(args, instance.AllowTLS)
		args = append(args, instance.SemiSyncEnforced)
		args = append(args, instance.InstanceAlias)
	}

	sql, err := mkInsertOdku("database_instance", columns, values, len(instances), insertIgnore)
	if err != nil {
		log.Fatalf("Failed to build query: %v", err)
	}

	return sql, args
}

// writeManyInstances stores instances in the orchestrator backend
func writeManyInstances(instances []*Instance, instanceWasActuallyFound bool, updateLastSeen bool) error {
	if len(instances) == 0 {
		return nil // nothing to write
	}

	sql, args := mkInsertOdkuForInstances(instances, instanceWasActuallyFound, updateLastSeen)

	if _, err := db.ExecOrchestrator(sql, args...); err != nil {
		return err
	}
	return nil
}

type instanceUpdateObject struct {
	instance                 *Instance
	instanceWasActuallyFound bool
	lastError                error
}

// instances sorter by instanceKey
type byInstanceKey []*Instance

func (a byInstanceKey) Len() int           { return len(a) }
func (a byInstanceKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byInstanceKey) Less(i, j int) bool { return a[i].Key.SmallerThan(&a[j].Key) }

var instanceWriteBuffer chan instanceUpdateObject
var forceFlushInstanceWriteBuffer = make(chan bool)

func enqueueInstanceWrite(instance *Instance, instanceWasActuallyFound bool, lastError error) {
	if len(instanceWriteBuffer) == config.Config.InstanceWriteBufferSize {
		// Signal the "flushing" gorouting that there's work.
		// We prefer doing all bulk flushes from one goroutine.
		forceFlushInstanceWriteBuffer <- true
	}
	instanceWriteBuffer <- instanceUpdateObject{instance, instanceWasActuallyFound, lastError}
}

// flushInstanceWriteBuffer saves enqueued instances to Orchestrator Db
func flushInstanceWriteBuffer() {
	var instances []*Instance
	var lastseen []*Instance // instances to update with last_seen field

	if len(instanceWriteBuffer) == 0 {
		return
	}

	for i := 0; i < len(instanceWriteBuffer); i++ {
		upd := <-instanceWriteBuffer
		if upd.instanceWasActuallyFound && upd.lastError == nil {
			lastseen = append(lastseen, upd.instance)
		} else {
			instances = append(instances, upd.instance)
			log.Debugf("writeInstance: will not update database_instance.last_seen due to error: %+v", upd.lastError)
		}
	}
	// sort instances by instanceKey (table pk) to make locking predictable
	sort.Sort(byInstanceKey(instances))
	sort.Sort(byInstanceKey(lastseen))

	writeFunc := func() error {
		err := writeManyInstances(instances, true, false)
		if err != nil {
			return log.Errorf("flushInstanceWriteBuffer writemany: %v", err)
		}
		err = writeManyInstances(lastseen, true, true)
		if err != nil {
			return log.Errorf("flushInstanceWriteBuffer last_seen: %v", err)
		}

		writeInstanceCounter.Inc(int64(len(instances)))
		return nil
	}
	err := ExecDBWriteFunc(writeFunc)
	if err != nil {
		log.Errorf("flushInstanceWriteBuffer: %v", err)
	}
}

// writeInstance stores an instance in the orchestrator backend
func writeInstance(instance *Instance, instanceWasActuallyFound bool, lastError error) error {
	if lastError != nil {
		log.Debugf("writeInstance: will not update database_instance due to error: %+v", lastError)
		return nil
	}
	return writeManyInstances([]*Instance{instance}, instanceWasActuallyFound, true)
}

// UpdateInstanceLastChecked updates the last_check timestamp in the orchestrator backed database
// for a given instance
func UpdateInstanceLastChecked(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	update
        		database_instance
        	set
        		last_checked = NOW()
			where
				hostname = ?
				and port = ?`,
			instanceKey.Hostname,
			instanceKey.Port,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// UpdateInstanceLastAttemptedCheck updates the last_attempted_check timestamp in the orchestrator backed database
// for a given instance.
// This is used as a failsafe mechanism in case access to the instance gets hung (it happens), in which case
// the entire ReadTopology gets stuck (and no, connection timeout nor driver timeouts don't help. Don't look at me,
// the world is a harsh place to live in).
// And so we make sure to note down *before* we even attempt to access the instance; and this raises a red flag when we
// wish to access the instance again: if last_attempted_check is *newer* than last_checked, that's bad news and means
// we have a "hanging" issue.
func UpdateInstanceLastAttemptedCheck(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	update
        		database_instance
        	set
        		last_attempted_check = NOW()
			where
				hostname = ?
				and port = ?`,
			instanceKey.Hostname,
			instanceKey.Port,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ForgetInstance removes an instance entry from the orchestrator backed database.
// It may be auto-rediscovered through topology or requested for discovery by multiple means.
func ForgetInstance(instanceKey *InstanceKey) error {
	_, err := db.ExecOrchestrator(`
			delete
				from database_instance
			where
				hostname = ? and port = ?`,
		instanceKey.Hostname,
		instanceKey.Port,
	)
	AuditOperation("forget", instanceKey, "")
	return err
}

// ForgetLongUnseenInstances will remove entries of all instacnes that have long since been last seen.
func ForgetLongUnseenInstances() error {
	sqlResult, err := db.ExecOrchestrator(`
			delete
				from database_instance
			where
				last_seen < NOW() - interval ? hour`,
		config.Config.UnseenInstanceForgetHours,
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("forget-unseen", nil, fmt.Sprintf("Forgotten instances: %d", rows))
	return err
}

// SnapshotTopologies records topology graph for all existing topologies
func SnapshotTopologies() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	insert ignore into
        		database_instance_topology_history (snapshot_unix_timestamp,
        			hostname, port, main_host, main_port, cluster_name, version)
        	select
        		UNIX_TIMESTAMP(NOW()),
        		hostname, port, main_host, main_port, cluster_name, version
			from
				database_instance
				`,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ReadHistoryClusterInstances reads (thin) instances from history
func ReadHistoryClusterInstances(clusterName string, historyTimestampPattern string) ([](*Instance), error) {
	instances := [](*Instance){}

	query := `
		select
			*
		from
			database_instance_topology_history
		where
			snapshot_unix_timestamp rlike ?
			and cluster_name = ?
		order by
			hostname, port`

	err := db.QueryOrchestrator(query, sqlutils.Args(historyTimestampPattern, clusterName), func(m sqlutils.RowMap) error {
		instance := NewInstance()

		instance.Key.Hostname = m.GetString("hostname")
		instance.Key.Port = m.GetInt("port")
		instance.MainKey.Hostname = m.GetString("main_host")
		instance.MainKey.Port = m.GetInt("main_port")
		instance.ClusterName = m.GetString("cluster_name")

		instances = append(instances, instance)
		return nil
	})
	if err != nil {
		return instances, log.Errore(err)
	}
	return instances, err
}

// RegisterCandidateInstance markes a given instance as suggested for successoring a main in the event of failover.
func RegisterCandidateInstance(instanceKey *InstanceKey, promotionRule CandidatePromotionRule) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
				insert into candidate_database_instance (
						hostname,
						port,
						last_suggested,
						promotion_rule)
					values (?, ?, NOW(), ?)
					on duplicate key update
						hostname=values(hostname),
						port=values(port),
						last_suggested=now(),
						promotion_rule=values(promotion_rule)
				`, instanceKey.Hostname, instanceKey.Port, string(promotionRule),
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ExpireCandidateInstances removes stale main candidate suggestions.
func ExpireCandidateInstances() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	delete from candidate_database_instance
				where last_suggested < NOW() - INTERVAL ? MINUTE
				`, config.Config.CandidateInstanceExpireMinutes,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// RecordInstanceCoordinatesHistory snapshots the binlog coordinates of instances
func RecordInstanceCoordinatesHistory() error {
	{
		writeFunc := func() error {
			_, err := db.ExecOrchestrator(`
        	delete from database_instance_coordinates_history
			where
				recorded_timestamp < NOW() - INTERVAL (? + 5) MINUTE
				`, config.Config.PseudoGTIDCoordinatesHistoryHeuristicMinutes,
			)
			return log.Errore(err)
		}
		ExecDBWriteFunc(writeFunc)
	}
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	insert into
        		database_instance_coordinates_history (
					hostname, port,	last_seen, recorded_timestamp,
					binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
				)
        	select
        		hostname, port, last_seen, NOW(),
				binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
			from
				database_instance
			where
				binary_log_file != ''
				OR relay_log_file != ''
				`,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// GetHeuristiclyRecentCoordinatesForInstance returns valid and reasonably recent coordinates for given instance.
func GetHeuristiclyRecentCoordinatesForInstance(instanceKey *InstanceKey) (selfCoordinates *BinlogCoordinates, relayLogCoordinates *BinlogCoordinates, err error) {
	query := `
		select
			binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
		from
			database_instance_coordinates_history
		where
			hostname = ?
			and port = ?
			and recorded_timestamp <= NOW() - INTERVAL ? MINUTE
		order by
			recorded_timestamp desc
			limit 1
			`
	err = db.QueryOrchestrator(query, sqlutils.Args(instanceKey.Hostname, instanceKey.Port, config.Config.PseudoGTIDCoordinatesHistoryHeuristicMinutes), func(m sqlutils.RowMap) error {
		selfCoordinates = &BinlogCoordinates{LogFile: m.GetString("binary_log_file"), LogPos: m.GetInt64("binary_log_pos")}
		relayLogCoordinates = &BinlogCoordinates{LogFile: m.GetString("relay_log_file"), LogPos: m.GetInt64("relay_log_pos")}

		return nil
	})
	return selfCoordinates, relayLogCoordinates, err
}

// RecordInstanceBinlogFileHistory snapshots the binlog coordinates of instances
func RecordInstanceBinlogFileHistory() error {
	{
		writeFunc := func() error {
			_, err := db.ExecOrchestrator(`
        	delete from database_instance_binlog_files_history
			where
				last_seen < NOW() - INTERVAL ? DAY
				`, config.Config.BinlogFileHistoryDays,
			)
			return log.Errore(err)
		}
		ExecDBWriteFunc(writeFunc)
	}
	if config.Config.BinlogFileHistoryDays == 0 {
		return nil
	}
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
      	insert into
      		database_instance_binlog_files_history (
					hostname, port,	first_seen, last_seen, binary_log_file, binary_log_pos
				)
      	select
      		hostname, port, last_seen, last_seen, binary_log_file, binary_log_pos
				from
					database_instance
				where
					binary_log_file != ''
				on duplicate key update
					last_seen = VALUES(last_seen),
					binary_log_pos = VALUES(binary_log_pos)
				`,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// UpdateInstanceRecentRelaylogHistory updates the database_instance_recent_relaylog_history
// table listing the current relaylog coordinates and the one-before.
// This information can be used to diagnoze a stale-replication scenario (for example, main is locked down
// and although subordinates are connected, they're not making progress)
func UpdateInstanceRecentRelaylogHistory() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	insert into
        		database_instance_recent_relaylog_history (
							hostname, port,
							current_relay_log_file, current_relay_log_pos, current_seen,
							prev_relay_log_file, prev_relay_log_pos
						)
						select
								hostname, port,
								relay_log_file, relay_log_pos, last_seen,
								'', 0
							from database_instance
							where
								relay_log_file != ''
						on duplicate key update
							prev_relay_log_file = current_relay_log_file,
							prev_relay_log_pos = current_relay_log_pos,
							prev_seen = current_seen,
							current_relay_log_file = values(current_relay_log_file),
							current_relay_log_pos = values (current_relay_log_pos),
							current_seen = values(current_seen)
				`,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}
