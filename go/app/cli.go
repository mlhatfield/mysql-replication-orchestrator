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

package app

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"regexp"
	"sort"
	"strings"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/util"
	"github.com/outbrain/orchestrator/go/agent"
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/inst"
	"github.com/outbrain/orchestrator/go/logic"
	"github.com/outbrain/orchestrator/go/process"
)

var thisInstanceKey *inst.InstanceKey
var knownCommands []CliCommand

type CliCommand struct {
	Command     string
	Section     string
	Description string
}

type stringSlice []string

func (a stringSlice) Len() int           { return len(a) }
func (a stringSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a stringSlice) Less(i, j int) bool { return a[i] < a[j] }

func registerCliCommand(command string, section string, description string) string {
	knownCommands = append(knownCommands, CliCommand{Command: command, Section: section, Description: description})
	return command
}

func commandsListing() string {
	listing := []string{}
	lastSection := ""
	for _, cliCommand := range knownCommands {
		if lastSection != cliCommand.Section {
			lastSection = cliCommand.Section
			listing = append(listing, fmt.Sprintf("%s:", cliCommand.Section))
		}
		commandListing := fmt.Sprintf("\t%-40s%s", cliCommand.Command, cliCommand.Description)
		listing = append(listing, commandListing)
	}
	return strings.Join(listing, "\n")
}

func availableCommandsUsage() string {
	return fmt.Sprintf(`Available commands (-c):
%+v
Run 'orchestrator' for full blown documentation

Usage for most commands:
	orchestrator -c <command> [-i <instance.fqdn>[,<instance.fqdn>]* ] [-d <destination.fqdn>] [--verbose|--debug]
`, commandsListing())
}

// getClusterName will make a best effort to deduce a cluster name using either a given alias
// or an instanceKey. First attempt is at alias, and if that doesn't work, we try instanceKey.
func getClusterName(clusterAlias string, instanceKey *inst.InstanceKey) (clusterName string) {
	var err error
	if clusterAlias != "" {
		if clusterName, err = inst.ReadClusterNameByAlias(clusterAlias); err == nil && clusterName != "" {
			return clusterName
		}
	}
	if clusterAlias != "" {
		// We're still here? So this wasn't an exact cluster name. Let's cehck if it's fuzzy:
		fuzzyInstanceKey, err := inst.ParseRawInstanceKeyLoose(clusterAlias)
		if err != nil {
			log.Fatale(err)
		}
		if clusterName, err = inst.FindClusterNameByFuzzyInstanceKey(fuzzyInstanceKey); clusterName == "" {
			log.Fatalf("Unable to determine cluster name by alias %+v", clusterAlias)
		}
		return clusterName
	}
	// So there is no alias. Let's check by instance key
	instanceKey = inst.ReadFuzzyInstanceKeyIfPossible(instanceKey)
	if instanceKey == nil {
		instanceKey = assignThisInstanceKey()
	}
	if instanceKey == nil {
		log.Fatalf("Unable to get cluster instances: unresolved instance")
	}
	instance := validateInstanceIsFound(instanceKey)
	clusterName = instance.ClusterName

	if clusterName == "" {
		log.Fatalf("Unable to determine cluster name")
	}
	return clusterName
}

func assignThisInstanceKey() *inst.InstanceKey {
	log.Debugf("Assuming instance is this machine, %+v", thisInstanceKey)
	return thisInstanceKey
}

// Common code to deduce the instance's instanceKey if not defined.
func deduceInstanceKeyIfNeeded(instance string, instanceKey *inst.InstanceKey, allowFuzzyMatch bool) *inst.InstanceKey {
	if allowFuzzyMatch {
		instanceKey = inst.ReadFuzzyInstanceKeyIfPossible(instanceKey)
	}
	if instanceKey == nil {
		instanceKey = assignThisInstanceKey()
	}
	if instanceKey == nil {
		log.Fatal("Cannot deduce instance:", instance)
	}
	return instanceKey
}

func validateInstanceIsFound(instanceKey *inst.InstanceKey) (instance *inst.Instance) {
	instance, _, err := inst.ReadInstance(instanceKey)
	if err != nil {
		log.Fatale(err)
	}
	if instance == nil {
		log.Fatalf("Instance not found: %+v", *instanceKey)
	}
	return instance
}

// CliWrapper is called from main and allows for the instance parameter
// to take multiple instance names separated by a comma or whitespace.
func CliWrapper(command string, strict bool, instances string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	r := regexp.MustCompile(`[ ,\r\n\t]+`)
	tokens := r.Split(instances, -1)
	switch command {
	case "submit-pool-instances":
		{
			// These commands unsplit the tokens (they expect a comma delimited list of instances)
			tokens = []string{instances}
		}
	}
	for _, instance := range tokens {
		if instance != "" || len(tokens) == 1 {
			Cli(command, strict, instance, destination, owner, reason, duration, pattern, clusterAlias, pool, hostnameFlag)
		}
	}
}

// Cli initiates a command line interface, executing requested command.
func Cli(command string, strict bool, instance string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	if instance != "" && !strings.Contains(instance, ":") {
		instance = fmt.Sprintf("%s:%d", instance, config.Config.DefaultInstancePort)
	}
	instanceKey, err := inst.ParseInstanceKey(instance)
	if err != nil {
		instanceKey = nil
	}
	rawInstanceKey, err := inst.NewRawInstanceKey(instance)
	if err != nil {
		rawInstanceKey = nil
	}

	if destination != "" && !strings.Contains(destination, ":") {
		destination = fmt.Sprintf("%s:%d", destination, config.Config.DefaultInstancePort)
	}
	destinationKey, err := inst.ParseInstanceKey(destination)
	if err != nil {
		destinationKey = nil
	}
	destinationKey = inst.ReadFuzzyInstanceKeyIfPossible(destinationKey)

	if hostname, err := os.Hostname(); err == nil {
		thisInstanceKey = &inst.InstanceKey{Hostname: hostname, Port: int(config.Config.DefaultInstancePort)}
	}
	postponedFunctionsContainer := inst.NewPostponedFunctionsContainer()

	if len(owner) == 0 {
		// get os username as owner
		usr, err := user.Current()
		if err != nil {
			log.Fatale(err)
		}
		owner = usr.Username
	}
	inst.SetMaintenanceOwner(owner)

	skipDatabaseCommands := false
	switch command {
	case "redeploy-internal-db":
		skipDatabaseCommands = true
	case "help":
		skipDatabaseCommands = true
	case "dump-config":
		skipDatabaseCommands = true
	}

	if !skipDatabaseCommands {
		process.ContinuousRegistration(string(process.OrchestratorExecutionCliMode), command)
	}
	// begin commands
	switch command {
	// smart mode
	case registerCliCommand("relocate", "Smart relocation", `Relocate a subordinate beneath another instance`), registerCliCommand("relocate-below", "Smart relocation", `Synonym to 'relocate', will be deprecated`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			_, err := inst.RelocateBelow(instanceKey, destinationKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), destinationKey.DisplayString()))
		}
	case registerCliCommand("relocate-subordinates", "Smart relocation", `Relocates all or part of the subordinates of a given instance under another instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			subordinates, _, err, errs := inst.RelocateSubordinates(instanceKey, destinationKey, pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, subordinate := range subordinates {
					fmt.Println(subordinate.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("regroup-subordinates", "Smart relocation", `Given an instance, pick one of its subordinate and make it local main of its siblings`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(instanceKey)

			lostSubordinates, equalSubordinates, aheadSubordinates, cannotReplicateSubordinates, promotedSubordinate, err := inst.RegroupSubordinates(instanceKey, false, func(candidateSubordinate *inst.Instance) { fmt.Println(candidateSubordinate.Key.DisplayString()) }, postponedFunctionsContainer)
			lostSubordinates = append(lostSubordinates, cannotReplicateSubordinates...)

			postponedFunctionsContainer.InvokePostponed()
			if promotedSubordinate == nil {
				log.Fatalf("Could not regroup subordinates of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(fmt.Sprintf("%s lost: %d, trivial: %d, pseudo-gtid: %d",
				promotedSubordinate.Key.DisplayString(), len(lostSubordinates), len(equalSubordinates), len(aheadSubordinates)))
			if err != nil {
				log.Fatale(err)
			}
		}
		// General replication commands
		// move, binlog file:pos
	case registerCliCommand("move-up", "Classic file:pos relocation", `Move a subordinate one level up the topology`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			instance, err := inst.MoveUp(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), instance.MainKey.DisplayString()))
		}
	case registerCliCommand("move-up-subordinates", "Classic file:pos relocation", `Moves subordinates of the given instance one level up the topology`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}

			movedSubordinates, _, err, errs := inst.MoveUpSubordinates(instanceKey, pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, subordinate := range movedSubordinates {
					fmt.Println(subordinate.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("move-below", "Classic file:pos relocation", `Moves a subordinate beneath its sibling. Both subordinates must be actively replicating from same main.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination/sibling:", destination)
			}
			_, err := inst.MoveBelow(instanceKey, destinationKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), destinationKey.DisplayString()))
		}
	case registerCliCommand("move-equivalent", "Classic file:pos relocation", `Moves a subordinate beneath another server, based on previously recorded "equivalence coordinates"`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			_, err := inst.MoveEquivalent(instanceKey, destinationKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), destinationKey.DisplayString()))
		}
	case registerCliCommand("repoint", "Classic file:pos relocation", `Make the given instance replicate from another instance without changing the binglog coordinates. Use with care`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			// destinationKey can be null, in which case the instance repoints to its existing main
			instance, err := inst.Repoint(instanceKey, destinationKey, inst.GTIDHintNeutral)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), instance.MainKey.DisplayString()))
		}
	case registerCliCommand("repoint-subordinates", "Classic file:pos relocation", `Repoint all subordinates of given instance to replicate back from the instance. Use with care`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			repointedSubordinates, err, errs := inst.RepointSubordinatesTo(instanceKey, pattern, destinationKey)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, subordinate := range repointedSubordinates {
					fmt.Println(fmt.Sprintf("%s<%s", subordinate.Key.DisplayString(), instanceKey.DisplayString()))
				}
			}
		}
	case registerCliCommand("ensubordinate-siblings", "Classic file:pos relocation", `Turn all siblings of a subordinate into its sub-subordinates.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			_, _, err := inst.EnsubordinateSiblings(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("ensubordinate-main", "Classic file:pos relocation", `Turn an instance into a main of its own main; essentially switch the two.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := inst.EnsubordinateMain(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("make-co-main", "Classic file:pos relocation", `Create a main-main replication. Given instance is a subordinate which replicates directly from a main.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.MakeCoMain(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("get-candidate-subordinate", "Classic file:pos relocation", `Information command suggesting the most up-to-date subordinate of a given instance that is good for promotion`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}

			instance, _, _, _, _, err := inst.GetCandidateSubordinate(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			} else {
				fmt.Println(instance.Key.DisplayString())
			}
		}
	case registerCliCommand("regroup-subordinates-bls", "Binlog server relocation", `Regroup Binlog Server subordinates of a given instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(instanceKey)

			_, promotedBinlogServer, err := inst.RegroupSubordinatesBinlogServers(instanceKey, false)
			if promotedBinlogServer == nil {
				log.Fatalf("Could not regroup binlog server subordinates of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(promotedBinlogServer.Key.DisplayString())
			if err != nil {
				log.Fatale(err)
			}
		}
	// move, GTID
	case registerCliCommand("move-gtid", "GTID relocation", `Move a subordinate beneath another instance.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			_, err := inst.MoveBelowGTID(instanceKey, destinationKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), destinationKey.DisplayString()))
		}
	case registerCliCommand("move-subordinates-gtid", "GTID relocation", `Moves all subordinates of a given instance under another (destination) instance using GTID`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			movedSubordinates, _, err, errs := inst.MoveSubordinatesGTID(instanceKey, destinationKey, pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, subordinate := range movedSubordinates {
					fmt.Println(subordinate.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("regroup-subordinates-gtid", "GTID relocation", `Given an instance, pick one of its subordinate and make it local main of its siblings, using GTID.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(instanceKey)

			lostSubordinates, movedSubordinates, cannotReplicateSubordinates, promotedSubordinate, err := inst.RegroupSubordinatesGTID(instanceKey, false, func(candidateSubordinate *inst.Instance) { fmt.Println(candidateSubordinate.Key.DisplayString()) })
			lostSubordinates = append(lostSubordinates, cannotReplicateSubordinates...)

			if promotedSubordinate == nil {
				log.Fatalf("Could not regroup subordinates of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(fmt.Sprintf("%s lost: %d, moved: %d",
				promotedSubordinate.Key.DisplayString(), len(lostSubordinates), len(movedSubordinates)))
			if err != nil {
				log.Fatale(err)
			}
		}
		// Pseudo-GTID
	case registerCliCommand("match", "Pseudo-GTID relocation", `Matches a subordinate beneath another (destination) instance using Pseudo-GTID`),
		registerCliCommand("match-below", "Pseudo-GTID relocation", `Synonym to 'match', will be deprecated`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			_, _, err := inst.MatchBelow(instanceKey, destinationKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), destinationKey.DisplayString()))
		}
	case registerCliCommand("match-up", "Pseudo-GTID relocation", `Transport the subordinate one level up the hierarchy, making it child of its grandparent, using Pseudo-GTID`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			instance, _, err := inst.MatchUp(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), instance.MainKey.DisplayString()))
		}
	case registerCliCommand("rematch", "Pseudo-GTID relocation", `Reconnect a subordinate onto its main, via PSeudo-GTID.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			instance, _, err := inst.RematchSubordinate(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), instance.MainKey.DisplayString()))
		}
	case registerCliCommand("match-subordinates", "Pseudo-GTID relocation", `Matches all subordinates of a given instance under another (destination) instance using Pseudo-GTID`),
		registerCliCommand("multi-match-subordinates", "Pseudo-GTID relocation", `Synonym to 'match-subordinates', will be deprecated`):
		{
			// Move all subordinates of "instance" beneath "destination"
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}

			matchedSubordinates, _, err, errs := inst.MultiMatchSubordinates(instanceKey, destinationKey, pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, subordinate := range matchedSubordinates {
					fmt.Println(subordinate.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("match-up-subordinates", "Pseudo-GTID relocation", `Matches subordinates of the given instance one level up the topology, making them siblings of given instance, using Pseudo-GTID`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}

			matchedSubordinates, _, err, errs := inst.MatchUpSubordinates(instanceKey, pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, subordinate := range matchedSubordinates {
					fmt.Println(subordinate.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("regroup-subordinates-pgtid", "Pseudo-GTID relocation", `Given an instance, pick one of its subordinate and make it local main of its siblings, using Pseudo-GTID.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(instanceKey)

			lostSubordinates, equalSubordinates, aheadSubordinates, cannotReplicateSubordinates, promotedSubordinate, err := inst.RegroupSubordinatesPseudoGTID(instanceKey, false, func(candidateSubordinate *inst.Instance) { fmt.Println(candidateSubordinate.Key.DisplayString()) }, postponedFunctionsContainer)
			lostSubordinates = append(lostSubordinates, cannotReplicateSubordinates...)
			postponedFunctionsContainer.InvokePostponed()
			if promotedSubordinate == nil {
				log.Fatalf("Could not regroup subordinates of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(fmt.Sprintf("%s lost: %d, trivial: %d, pseudo-gtid: %d",
				promotedSubordinate.Key.DisplayString(), len(lostSubordinates), len(equalSubordinates), len(aheadSubordinates)))
			if err != nil {
				log.Fatale(err)
			}
		}
		// General replication commands
	case registerCliCommand("enable-gtid", "Replication, general", `If possible, turn on GTID replication`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.EnableGTID(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("disable-gtid", "Replication, general", `Turn off GTID replication, back to file:pos replication`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.DisableGTID(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("reset-main-gtid-remove-own-uuid", "Replication, general", `Reset main on instance, remove GTID entries generated by instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.ResetMainGTIDOperation(instanceKey, true, "")
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("skip-query", "Replication, general", `Skip a single statement on a subordinate; either when running with GTID or without`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.SkipQuery(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("stop-subordinate", "Replication, general", `Issue a STOP SLAVE on an instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.StopSubordinate(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("start-subordinate", "Replication, general", `Issue a START SLAVE on an instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.StartSubordinate(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("restart-subordinate", "Replication, general", `STOP and START SLAVE on an instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.RestartSubordinate(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("reset-subordinate", "Replication, general", `Issues a RESET SLAVE command; use with care`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.ResetSubordinateOperation(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("detach-subordinate", "Replication, general", `Stops replication and modifies binlog position into an impossible, yet reversible, value.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.DetachSubordinateOperation(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("reattach-subordinate", "Replication, general", `Undo a detach-subordinate operation`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.ReattachSubordinateOperation(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("detach-subordinate-main-host", "Replication, general", `Stops replication and modifies Main_Host into an impossible, yet reversible, value.`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := inst.DetachSubordinateMainHost(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("reattach-subordinate-main-host", "Replication, general", `Undo a detach-subordinate-main-host operation`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := inst.ReattachSubordinateMainHost(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("main-pos-wait", "Replication, general", `Wait until subordinate reaches given replication coordinates (--binlog=file:pos)`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := inst.ReadTopologyInstanceUnbuffered(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			var binlogCoordinates *inst.BinlogCoordinates

			if binlogCoordinates, err = inst.ParseBinlogCoordinates(*config.RuntimeCLIFlags.BinlogFile); err != nil {
				log.Fatalf("Expecing --binlog argument as file:pos")
			}
			_, err = inst.MainPosWait(instanceKey, binlogCoordinates)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("restart-subordinate-statements", "Replication, general", `Get a list of statements to execute to stop then restore subordinate to same execution state. Provide --statement for injected statement`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			statements, err := inst.GetSubordinateRestartPreserveStatements(instanceKey, *config.RuntimeCLIFlags.Statement)
			if err != nil {
				log.Fatale(err)
			}
			for _, statement := range statements {
				fmt.Println(statement)
			}
		}
		// Instance
	case registerCliCommand("set-read-only", "Instance", `Turn an instance read-only, via SET GLOBAL read_only := 1`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.SetReadOnly(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("set-writeable", "Instance", `Turn an instance writeable, via SET GLOBAL read_only := 0`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			_, err := inst.SetReadOnly(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
		// Binary log operations
	case registerCliCommand("flush-binary-logs", "Binary logs", `Flush binary logs on an instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			var err error
			if *config.RuntimeCLIFlags.BinlogFile == "" {
				_, err = inst.FlushBinaryLogs(instanceKey, 1)
			} else {
				_, err = inst.FlushBinaryLogsTo(instanceKey, *config.RuntimeCLIFlags.BinlogFile)
			}
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("purge-binary-logs", "Binary logs", `Purge binary logs of an instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			var err error
			if *config.RuntimeCLIFlags.BinlogFile == "" {
				log.Fatal("expecting --binlog value")
			}

			_, err = inst.PurgeBinaryLogsTo(instanceKey, *config.RuntimeCLIFlags.BinlogFile)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("last-pseudo-gtid", "Binary logs", `Find latest Pseudo-GTID entry in instance's binary logs`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := inst.ReadTopologyInstanceUnbuffered(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			coordinates, text, err := inst.FindLastPseudoGTIDEntry(instance, instance.RelaylogCoordinates, nil, strict, nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v:%s", *coordinates, text))
		}
	case registerCliCommand("find-binlog-entry", "Binary logs", `Get binlog file:pos of entry given by --pattern (exact full match, not a regular expression) in a given instance`):
		{
			if pattern == "" {
				log.Fatal("No pattern given")
			}
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := inst.ReadTopologyInstanceUnbuffered(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			coordinates, err := inst.SearchEntryInInstanceBinlogs(instance, pattern, false, nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v", *coordinates))
		}
	case registerCliCommand("correlate-binlog-pos", "Binary logs", `Given an instance (-i) and binlog coordinates (--binlog=file:pos), find the correlated coordinates in another instance (-d)`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := inst.ReadTopologyInstanceUnbuffered(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			if !instance.LogBinEnabled {
				log.Fatalf("Instance does not have binary logs: %+v", *instanceKey)
			}
			if destinationKey == nil {
				log.Fatal("Cannot deduce target instance:", destination)
			}
			otherInstance, err := inst.ReadTopologyInstanceUnbuffered(destinationKey)
			if err != nil {
				log.Fatale(err)
			}
			if otherInstance == nil {
				log.Fatalf("Instance not found: %+v", *destinationKey)
			}
			var binlogCoordinates *inst.BinlogCoordinates
			if *config.RuntimeCLIFlags.BinlogFile == "" {
				binlogCoordinates = &instance.SelfBinlogCoordinates
			} else {
				if binlogCoordinates, err = inst.ParseBinlogCoordinates(*config.RuntimeCLIFlags.BinlogFile); err != nil {
					log.Fatalf("Expecing --binlog argument as file:pos")
				}
			}

			coordinates, _, err := inst.CorrelateBinlogCoordinates(instance, binlogCoordinates, otherInstance)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v", *coordinates))
		}
		// Pool
	case registerCliCommand("submit-pool-instances", "Pools", `Submit a pool name with a list of instances in that pool`):
		{
			if pool == "" {
				log.Fatal("Please submit --pool")
			}
			err := inst.ApplyPoolInstances(pool, instance)
			if err != nil {
				log.Fatale(err)
			}
		}
	case registerCliCommand("cluster-pool-instances", "Pools", `List all pools and their associated instances`):
		{
			clusterPoolInstances, err := inst.ReadAllClusterPoolInstances()
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterPoolInstance := range clusterPoolInstances {
				fmt.Println(fmt.Sprintf("%s\t%s\t%s\t%s:%d", clusterPoolInstance.ClusterName, clusterPoolInstance.ClusterAlias, clusterPoolInstance.Pool, clusterPoolInstance.Hostname, clusterPoolInstance.Port))
			}
		}
	case registerCliCommand("which-heuristic-cluster-pool-instances", "Pools", `List instances of a given cluster which are in either any pool or in a specific pool`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)

			instances, err := inst.GetHeuristicClusterPoolInstances(clusterName, pool)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
		// Information
	case registerCliCommand("find", "Information", `Find instances whose hostname matches given regex pattern`):
		{
			if pattern == "" {
				log.Fatal("No pattern given")
			}
			instances, err := inst.FindInstances(pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("clusters", "Information", `List all clusters known to orchestrator`):
		{
			clusters, err := inst.ReadClusters()
			if err != nil {
				log.Fatale(err)
			} else {
				fmt.Println(strings.Join(clusters, "\n"))
			}
		}
	case registerCliCommand("all-clusters-mains", "Information", `List of writeable mains, one per cluster`):
		{
			instances, err := inst.ReadWriteableClustersMains()
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("topology", "Information", `Show an ascii-graph of a replication topology, given a member of that topology`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			output, err := inst.ASCIITopology(clusterName, pattern)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(output)
		}
	case registerCliCommand("all-instances", "Information", `The complete list of known instances`):
		{
			instances, err := inst.FindInstances(".")
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("which-instance", "Information", `Output the fully-qualified hostname:port representation of the given instance, or error if unknown`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unable to get main: unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			fmt.Println(instance.Key.DisplayString())
		}
	case registerCliCommand("which-cluster", "Information", `Output the name of the cluster an instance belongs to, or error if unknown to orchestrator`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			fmt.Println(clusterName)
		}
	case registerCliCommand("which-cluster-domain", "Information", `Output the domain name of the cluster an instance belongs to, or error if unknown to orchestrator`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			clusterInfo, err := inst.ReadClusterInfo(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(clusterInfo.ClusterDomain)
		}
	case registerCliCommand("which-heuristic-domain-instance", "Information", `Returns the instance associated as the cluster's writer with a cluster's domain name.`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instanceKey, err := inst.GetHeuristicClusterDomainInstanceAttribute(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("which-cluster-main", "Information", `Output the name of the main in a given cluster`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			mains, err := inst.ReadClusterWriteableMain(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			if len(mains) == 0 {
				log.Fatalf("No writeable mains found for cluster %+v", clusterName)
			}
			fmt.Println(mains[0].Key.DisplayString())
		}
	case registerCliCommand("which-cluster-instances", "Information", `Output the list of instances participating in same cluster as given instance`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instances, err := inst.ReadClusterInstances(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-cluster-osc-subordinates", "Information", `Output a list of subordinates in a cluster, that could serve as a pt-online-schema-change operation control subordinates`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instances, err := inst.GetClusterOSCSubordinates(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-cluster-gh-ost-subordinates", "Information", `Output a list of subordinates in a cluster, that could serve as a gh-ost working server`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instances, err := inst.GetClusterGhostSubordinates(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-main", "Information", `Output the fully-qualified hostname:port representation of a given instance's main`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unable to get main: unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			if instance.MainKey.IsValid() {
				fmt.Println(instance.MainKey.DisplayString())
			}
		}
	case registerCliCommand("which-subordinates", "Information", `Output the fully-qualified hostname:port list of subordinates of a given instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unable to get subordinates: unresolved instance")
			}
			subordinates, err := inst.ReadSubordinateInstances(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			for _, subordinate := range subordinates {
				fmt.Println(subordinate.Key.DisplayString())
			}
		}
	case registerCliCommand("which-lost-in-recovery", "Information", `List instances marked as downtimed for being lost in a recovery process`):
		{
			instances, err := inst.ReadLostInRecoveryInstances("")
			if err != nil {
				log.Fatale(err)
			}
			for _, instance := range instances {
				fmt.Println(instance.Key.DisplayString())
			}
		}
	case registerCliCommand("instance-status", "Information", `Output short status on a given instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatalf("Unable to get status: unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			fmt.Println(instance.HumanReadableDescription())
		}
	case registerCliCommand("get-cluster-heuristic-lag", "Information", `For a given cluster (indicated by an instance or alias), output a heuristic "representative" lag of that cluster`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			lag, err := inst.GetClusterHeuristicLag(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(lag)
		}
		// Instance management
	case registerCliCommand("discover", "Instance management", `Lookup an instance, investigate it`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, false)
			instance, err := inst.ReadTopologyInstanceUnbuffered(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instance.Key.DisplayString())
		}
	case registerCliCommand("forget", "Instance management", `Forget about an instance's existence`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if rawInstanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			err := inst.ForgetInstance(rawInstanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(rawInstanceKey.DisplayString())
		}
	case registerCliCommand("begin-maintenance", "Instance management", `Request a maintenance lock on an instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if reason == "" {
				log.Fatal("--reason option required")
			}
			var durationSeconds int = 0
			if duration != "" {
				durationSeconds, err = util.SimpleTimeToSeconds(duration)
				if err != nil {
					log.Fatale(err)
				}
				if durationSeconds < 0 {
					log.Fatalf("Duration value must be non-negative. Given value: %d", durationSeconds)
				}
			}
			maintenanceKey, err := inst.BeginBoundedMaintenance(instanceKey, inst.GetMaintenanceOwner(), reason, uint(durationSeconds), true)
			if err == nil {
				log.Infof("Maintenance key: %+v", maintenanceKey)
				log.Infof("Maintenance duration: %d seconds", durationSeconds)
			}
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("end-maintenance", "Instance management", `Remove maintenance lock from an instance`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			err := inst.EndMaintenanceByInstanceKey(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("begin-downtime", "Instance management", `Mark an instance as downtimed`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if reason == "" {
				log.Fatal("--reason option required")
			}
			var durationSeconds int = 0
			if duration != "" {
				durationSeconds, err = util.SimpleTimeToSeconds(duration)
				if err != nil {
					log.Fatale(err)
				}
				if durationSeconds < 0 {
					log.Fatalf("Duration value must be non-negative. Given value: %d", durationSeconds)
				}
			}
			err := inst.BeginDowntime(instanceKey, inst.GetMaintenanceOwner(), reason, uint(durationSeconds))
			if err == nil {
				log.Infof("Downtime duration: %d seconds", durationSeconds)
			} else {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("end-downtime", "Instance management", `Indicate an instance is no longer downtimed`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			err := inst.EndDowntime(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
		// Recovery & analysis
	case registerCliCommand("recover", "Recovery", `Do auto-recovery given a dead instance`), registerCliCommand("recover-lite", "Recovery", `Do auto-recovery given a dead instance. Orchestrator chooses the best course of actionwithout executing external processes`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}

			recoveryAttempted, promotedInstanceKey, err := logic.CheckAndRecover(instanceKey, destinationKey, (command == "recover-lite"))
			if err != nil {
				log.Fatale(err)
			}
			if recoveryAttempted {
				if promotedInstanceKey == nil {
					log.Fatalf("Recovery attempted yet no subordinate promoted")
				}
				fmt.Println(promotedInstanceKey.DisplayString())
			}
		}
	case registerCliCommand("force-main-takeover", "Recovery", `Forcibly discard main and promote another (direct child) instance instead, even if everything is running well`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination, the instance to promote in place of the main. Please provide with -d")
			}
			destination := validateInstanceIsFound(destinationKey)
			topologyRecovery, err := logic.ForceMainTakeover(clusterName, destination)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
		}
	case registerCliCommand("graceful-main-takeover", "Recovery", `Gracefully discard main and promote another (direct child) instance instead, even if everything is running well`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			topologyRecovery, promotedMainCoordinates, err := logic.GracefulMainTakeover(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
			fmt.Println(*promotedMainCoordinates)
			log.Debugf("Promoted %+v as new main. Binlog coordinates at time of promotion: %+v", topologyRecovery.SuccessorKey, *promotedMainCoordinates)
		}
	case registerCliCommand("replication-analysis", "Recovery", `Request an analysis of potential crash incidents in all known topologies`):
		{
			analysis, err := inst.GetReplicationAnalysis("", false, false)
			if err != nil {
				log.Fatale(err)
			}
			for _, entry := range analysis {
				fmt.Println(fmt.Sprintf("%s (cluster %s): %s", entry.AnalyzedInstanceKey.DisplayString(), entry.ClusterDetails.ClusterName, entry.AnalysisString()))
			}
		}
	case registerCliCommand("ack-cluster-recoveries", "Recovery", `Acknowledge recoveries for a given cluster; this unblocks pending future recoveries`):
		{
			if reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			clusterName := getClusterName(clusterAlias, instanceKey)
			countRecoveries, err := logic.AcknowledgeClusterRecoveries(clusterName, inst.GetMaintenanceOwner(), reason)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%d recoveries acknowldged", countRecoveries))
		}
	case registerCliCommand("ack-instance-recoveries", "Recovery", `Acknowledge recoveries for a given instance; this unblocks pending future recoveries`):
		{
			if reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)

			countRecoveries, err := logic.AcknowledgeInstanceRecoveries(instanceKey, inst.GetMaintenanceOwner(), reason)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%d recoveries acknowldged", countRecoveries))
		}
	// Instance meta
	case registerCliCommand("register-candidate", "Instance, meta", `Indicate that a specific instance is a preferred candidate for main promotion`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			promotionRule, err := inst.ParseCandidatePromotionRule(*config.RuntimeCLIFlags.PromotionRule)
			if err != nil {
				log.Fatale(err)
			}
			err = inst.RegisterCandidateInstance(instanceKey, promotionRule)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("register-hostname-unresolve", "Instance, meta", `Assigns the given instance a virtual (aka "unresolved") name`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			err := inst.RegisterHostnameUnresolve(instanceKey, hostnameFlag)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("deregister-hostname-unresolve", "Instance, meta", `Explicitly deregister/dosassociate a hostname with an "unresolved" name`):
		{
			instanceKey = deduceInstanceKeyIfNeeded(instance, instanceKey, true)
			err := inst.DeregisterHostnameUnresolve(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("set-heuristic-domain-instance", "Instance, meta", `Associate domain name of given cluster with what seems to be the writer main for that cluster`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instanceKey, err := inst.HeuristicallyApplyClusterDomainInstanceAttribute(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}

		// meta
	case registerCliCommand("snapshot-topologies", "Meta", `Take a snapshot of existing topologies.`):
		{
			err := inst.SnapshotTopologies()
			if err != nil {
				log.Fatale(err)
			}
		}
	case registerCliCommand("continuous", "Meta", `Enter continuous mode, and actively poll for instances, diagnose problems, do maintenance`):
		{
			logic.ContinuousDiscovery()
		}
	case registerCliCommand("active-nodes", "Meta", `List currently active orchestrator nodes`):
		{
			nodes, err := process.ReadAvailableNodes(false)
			if err != nil {
				log.Fatale(err)
			}
			for _, node := range nodes {
				fmt.Println(node)
			}
		}
	case registerCliCommand("access-token", "Meta", `Get a HTTP access token`):
		{
			publicToken, err := process.GenerateAccessToken(owner)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(publicToken)
		}
	case registerCliCommand("resolve", "Meta", `Resolve given hostname`):
		{
			if rawInstanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			if conn, err := net.Dial("tcp", rawInstanceKey.DisplayString()); err == nil {
				log.Debugf("tcp test is good; got connection %+v", conn)
				conn.Close()
			} else {
				log.Fatale(err)
			}
			if cname, err := inst.GetCNAME(rawInstanceKey.Hostname); err == nil {
				log.Debugf("GetCNAME() %+v, %+v", cname, err)
				rawInstanceKey.Hostname = cname
				fmt.Println(rawInstanceKey.DisplayString())
			} else {
				log.Fatale(err)
			}
		}
	case registerCliCommand("reset-hostname-resolve-cache", "Meta", `Clear the hostname resolve cache`):
		{
			err := inst.ResetHostnameResolveCache()
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println("hostname resolve cache cleared")
		}
	case registerCliCommand("dump-config", "Meta", `Print out configuration in JSON format`):
		{
			jsonString := config.Config.ToJSONString()
			fmt.Println(jsonString)
		}
	case registerCliCommand("redeploy-internal-db", "Meta, internal", `Force internal schema migration to current backend structure`):
		{
			config.RuntimeCLIFlags.ConfiguredVersion = ""
			inst.ReadClusters()
			fmt.Println("Redeployed internal db")
		}
	case registerCliCommand("custom-command", "Agent", "Execute a custom command on the agent as defined in the agent conf"):
		{
			output, err := agent.CustomCommand(hostnameFlag, pattern)
			if err != nil {
				log.Fatale(err)
			}

			fmt.Printf("%v\n", output)
		}
	case registerCliCommand("disable-global-recoveries", "", `Disallow orchestrator from performing recoveries globally`):
		{
			if err := logic.DisableRecovery(); err != nil {
				log.Fatalf("ERROR: Failed to disable recoveries globally: %v\n", err)
			}
			fmt.Println("OK: Orchestrator recoveries DISABLED globally")
		}
	case registerCliCommand("enable-global-recoveries", "", `Allow orchestrator to perform recoveries globally`):
		{
			if err := logic.EnableRecovery(); err != nil {
				log.Fatalf("ERROR: Failed to enable recoveries globally: %v\n", err)
			}
			fmt.Println("OK: Orchestrator recoveries ENABLED globally")
		}
	case registerCliCommand("check-global-recoveries", "", `Show the global recovery configuration`):
		{
			isDisabled, err := logic.IsRecoveryDisabled()
			if err != nil {
				log.Fatalf("ERROR: Failed to determine if recoveries are disabled globally: %v\n", err)
			}
			fmt.Printf("OK: Global recoveries disabled: %v\n", isDisabled)
		}
	case registerCliCommand("bulk-instances", "", `Return a list of sorted instance names known to orchestrator`):
		{
			instances, err := inst.BulkReadInstance()
			if err != nil {
				log.Fatalf("Error: Failed to retrieve instances: %v\n", err)
				return
			}
			var asciiInstances stringSlice
			for _, v := range instances {
				asciiInstances = append(asciiInstances, v.String())
			}
			sort.Sort(asciiInstances)
			fmt.Printf("%s\n", strings.Join(asciiInstances, "\n"))
		}
	case registerCliCommand("bulk-promotion-rules", "", `Return a list of promotion rules known to orchestrator`):
		{
			promotionRules, err := inst.BulkReadCandidateDatabaseInstance()
			if err != nil {
				log.Fatalf("Error: Failed to retrieve promotion rules: %v\n", err)
			}
			var asciiPromotionRules stringSlice
			for _, v := range promotionRules {
				asciiPromotionRules = append(asciiPromotionRules, v.String())
			}
			sort.Sort(asciiPromotionRules)

			fmt.Printf("%s\n", strings.Join(asciiPromotionRules, "\n"))
		}
	case registerCliCommand("show-resolve-hosts", "Meta, internal", `Show the content of the hostname_resolve table. Generally used for debugging`):
		{
			inst.ShowAllHostnameResolves()
		}
	case registerCliCommand("show-unresolve-hosts", "Meta, internal", `Show the content of the hostname_unresolve table. Generally used for debugging`):
		{
			inst.ShowAllHostnameUnresolves()
		}
		// Help
	case "help":
		{
			fmt.Fprintf(os.Stderr, availableCommandsUsage())
		}
	default:
		log.Fatalf("Unknown command: \"%s\". %s", command, availableCommandsUsage())
	}
}
