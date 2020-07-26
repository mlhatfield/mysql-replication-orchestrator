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

package http

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"

	"github.com/outbrain/golib/util"

	"github.com/outbrain/orchestrator/go/agent"
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/inst"
	"github.com/outbrain/orchestrator/go/logic"
	"github.com/outbrain/orchestrator/go/process"
)

// APIResponseCode is an OK/ERROR response code
type APIResponseCode int

const (
	ERROR APIResponseCode = iota
	OK
)

func (this *APIResponseCode) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.String())
}

func (this *APIResponseCode) String() string {
	switch *this {
	case ERROR:
		return "ERROR"
	case OK:
		return "OK"
	}
	return "unknown"
}

// APIResponse is a response returned as JSON to various requests.
type APIResponse struct {
	Code    APIResponseCode
	Message string
	Details interface{}
}

type HttpAPI struct {
	URLPrefix string
}

var API HttpAPI = HttpAPI{}

func (this *HttpAPI) getInstanceKey(host string, port string) (inst.InstanceKey, error) {
	instanceKey, err := inst.NewInstanceKeyFromStrings(host, port)
	return *instanceKey, err
}

func (this *HttpAPI) getBinlogCoordinates(logFile string, logPos string) (inst.BinlogCoordinates, error) {
	coordinates := inst.BinlogCoordinates{LogFile: logFile}
	var err error
	if coordinates.LogPos, err = strconv.ParseInt(logPos, 10, 0); err != nil {
		return coordinates, fmt.Errorf("Invalid logPos: %s", logPos)
	}

	return coordinates, err
}

// Instance reads and returns an instance's details.
func (this *HttpAPI) Instance(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	r.JSON(200, instance)
}

// Discover issues a synchronous read on an instance
func (this *HttpAPI) Discover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ReadTopologyInstanceUnbuffered(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance discovered: %+v", instance.Key), Details: instance})
}

// Refresh synchronuously re-reads a topology instance
func (this *HttpAPI) Refresh(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	_, err = inst.RefreshTopologyInstance(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance refreshed: %+v", instanceKey), Details: instanceKey})
}

// Forget removes an instance entry fro backend database
func (this *HttpAPI) Forget(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	// We ignore errors: we're looking to do a destructive operation anyhow.
	rawInstanceKey, _ := inst.NewRawInstanceKey(fmt.Sprintf("%s:%s", params["host"], params["port"]))

	inst.ForgetInstance(rawInstanceKey)

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance forgotten: %+v", *rawInstanceKey)})
}

// Resolve tries to resolve hostname and then checks to see if port is open on that host.
func (this *HttpAPI) Resolve(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if conn, err := net.Dial("tcp", instanceKey.DisplayString()); err == nil {
		conn.Close()
	} else {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Instance resolved", Details: instanceKey})
}

// BeginMaintenance begins maintenance mode for given instance
func (this *HttpAPI) BeginMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	key, err := inst.BeginBoundedMaintenance(&instanceKey, params["owner"], params["reason"], 0, true)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error(), Details: key})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance begun: %+v", instanceKey)})
}

// EndMaintenance terminates maintenance mode
func (this *HttpAPI) EndMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	maintenanceKey, err := strconv.ParseInt(params["maintenanceKey"], 10, 0)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	err = inst.EndMaintenance(maintenanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance ended: %+v", maintenanceKey)})
}

// EndMaintenanceByInstanceKey terminates maintenance mode for given instance
func (this *HttpAPI) EndMaintenanceByInstanceKey(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	err = inst.EndMaintenanceByInstanceKey(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance ended: %+v", instanceKey)})
}

// Maintenance provides list of instance under active maintenance
func (this *HttpAPI) Maintenance(params martini.Params, r render.Render, req *http.Request) {
	instanceKeys, err := inst.ReadActiveMaintenance()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instanceKeys)
}

// BeginDowntime sets a downtime flag with default duration
func (this *HttpAPI) BeginDowntime(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	var durationSeconds int = 0
	if params["duration"] != "" {
		durationSeconds, err = util.SimpleTimeToSeconds(params["duration"])
		if durationSeconds < 0 {
			err = fmt.Errorf("Duration value must be non-negative. Given value: %d", durationSeconds)
		}
		if err != nil {
			r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
			return
		}
	}

	err = inst.BeginDowntime(&instanceKey, params["owner"], params["reason"], uint(durationSeconds))

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error(), Details: instanceKey})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Downtime begun: %+v", instanceKey)})
}

// EndDowntime terminates downtime (removes downtime flag) for an instance
func (this *HttpAPI) EndDowntime(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	err = inst.EndDowntime(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Downtime ended: %+v", instanceKey)})
}

// MoveUp attempts to move an instance up the topology
func (this *HttpAPI) MoveUp(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.MoveUp(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved up", instanceKey), Details: instance})
}

// MoveUpSubordinates attempts to move up all subordinates of an instance
func (this *HttpAPI) MoveUpSubordinates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	subordinates, newMain, err, errs := inst.MoveUpSubordinates(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Moved up %d subordinates of %+v below %+v; %d errors: %+v", len(subordinates), instanceKey, newMain.Key, len(errs), errs), Details: newMain.Key})
}

// MoveUpSubordinates attempts to move up all subordinates of an instance
func (this *HttpAPI) RepointSubordinates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	subordinates, err, _ := inst.RepointSubordinates(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Repointed %d subordinates of %+v", len(subordinates), instanceKey), Details: instanceKey})
}

// MakeCoMain attempts to make an instance co-main with its own main
func (this *HttpAPI) MakeCoMain(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.MakeCoMain(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance made co-main: %+v", instance.Key), Details: instance})
}

// ResetSubordinate makes a subordinate forget about its main, effectively breaking the replication
func (this *HttpAPI) ResetSubordinate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ResetSubordinateOperation(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Subordinate reset on %+v", instance.Key), Details: instance})
}

// DetachSubordinate corrupts a subordinate's binlog corrdinates (though encodes it in such way
// that is reversible), effectively breaking replication
func (this *HttpAPI) DetachSubordinate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.DetachSubordinateOperation(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Subordinate detached: %+v", instance.Key), Details: instance})
}

// ReattachSubordinate reverts a DetachSubordinate commands by reassigning the correct
// binlog coordinates to an instance
func (this *HttpAPI) ReattachSubordinate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ReattachSubordinateOperation(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Subordinate reattached: %+v", instance.Key), Details: instance})
}

// ReattachSubordinateMainHost reverts a DetachSubordinateMainHost command
// by resoting the original main hostname in CHANGE MASTER TO
func (this *HttpAPI) ReattachSubordinateMainHost(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ReattachSubordinateMainHost(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Subordinate reattached: %+v", instance.Key), Details: instance})
}

// EnableGTID attempts to enable GTID on a subordinate
func (this *HttpAPI) EnableGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.EnableGTID(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Enabled GTID on %+v", instance.Key), Details: instance})
}

// DisableGTID attempts to disable GTID on a subordinate, and revert to binlog file:pos
func (this *HttpAPI) DisableGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.DisableGTID(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Disabled GTID on %+v", instance.Key), Details: instance})
}

// MoveBelow attempts to move an instance below its supposed sibling
func (this *HttpAPI) MoveBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	siblingKey, err := this.getInstanceKey(params["siblingHost"], params["siblingPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MoveBelow(&instanceKey, &siblingKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved below %+v", instanceKey, siblingKey), Details: instance})
}

// MoveBelowGTID attempts to move an instance below another, via GTID
func (this *HttpAPI) MoveBelowGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MoveBelowGTID(&instanceKey, &belowKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved below %+v via GTID", instanceKey, belowKey), Details: instance})
}

// MoveSubordinatesGTID attempts to move an instance below another, via GTID
func (this *HttpAPI) MoveSubordinatesGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	movedSubordinates, _, err, errs := inst.MoveSubordinatesGTID(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Moved %d subordinates of %+v below %+v via GTID; %d errors: %+v", len(movedSubordinates), instanceKey, belowKey, len(errs), errs), Details: belowKey})
}

// EnsubordinateSiblings
func (this *HttpAPI) EnsubordinateSiblings(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, count, err := inst.EnsubordinateSiblings(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Ensubordinated %d siblings of %+v", count, instanceKey), Details: instance})
}

// EnsubordinateMain
func (this *HttpAPI) EnsubordinateMain(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.EnsubordinateMain(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("%+v ensubordinated its main", instanceKey), Details: instance})
}

// RelocateBelow attempts to move an instance below another, orchestrator choosing the best (potentially multi-step)
// relocation method
func (this *HttpAPI) RelocateBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.RelocateBelow(&instanceKey, &belowKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v relocated below %+v", instanceKey, belowKey), Details: instance})
}

// RelocateSubordinates attempts to smartly relocate subordinates of a given instance below another
func (this *HttpAPI) RelocateSubordinates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	subordinates, _, err, errs := inst.RelocateSubordinates(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Relocated %d subordinates of %+v below %+v; %d errors: %+v", len(subordinates), instanceKey, belowKey, len(errs), errs), Details: subordinates})
}

// MoveEquivalent attempts to move an instance below another, baseed on known equivalence main coordinates
func (this *HttpAPI) MoveEquivalent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MoveEquivalent(&instanceKey, &belowKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v relocated via equivalence coordinates below %+v", instanceKey, belowKey), Details: instance})
}

// LastPseudoGTID attempts to find the last pseugo-gtid entry in an instance
func (this *HttpAPI) LastPseudoGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.ReadTopologyInstanceUnbuffered(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if instance == nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Instance not found: %+v", instanceKey)})
		return
	}
	coordinates, text, err := inst.FindLastPseudoGTIDEntry(instance, instance.RelaylogCoordinates, nil, false, nil)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("%+v", *coordinates), Details: text})
}

// MatchBelow attempts to move an instance below another via pseudo GTID matching of binlog entries
func (this *HttpAPI) MatchBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, matchedCoordinates, err := inst.MatchBelow(&instanceKey, &belowKey, true)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v matched below %+v at %+v", instanceKey, belowKey, *matchedCoordinates), Details: instance})
}

// MatchBelow attempts to move an instance below another via pseudo GTID matching of binlog entries
func (this *HttpAPI) MatchUp(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, matchedCoordinates, err := inst.MatchUp(&instanceKey, true)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v matched up at %+v", instanceKey, *matchedCoordinates), Details: instance})
}

// MultiMatchSubordinates attempts to match all subordinates of a given instance below another, efficiently
func (this *HttpAPI) MultiMatchSubordinates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := this.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	subordinates, newMain, err, errs := inst.MultiMatchSubordinates(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Matched %d subordinates of %+v below %+v; %d errors: %+v", len(subordinates), instanceKey, newMain.Key, len(errs), errs), Details: newMain.Key})
}

// MatchUpSubordinates attempts to match up all subordinates of an instance
func (this *HttpAPI) MatchUpSubordinates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	subordinates, newMain, err, errs := inst.MatchUpSubordinates(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Matched up %d subordinates of %+v below %+v; %d errors: %+v", len(subordinates), instanceKey, newMain.Key, len(errs), errs), Details: newMain.Key})
}

// RegroupSubordinates attempts to pick a subordinate of a given instance and make it ensubordinate its siblings, using any
// method possible (GTID, Pseudo-GTID, binlog servers)
func (this *HttpAPI) RegroupSubordinates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	lostSubordinates, equalSubordinates, aheadSubordinates, cannotReplicateSubordinates, promotedSubordinate, err := inst.RegroupSubordinates(&instanceKey, false, nil, nil)
	lostSubordinates = append(lostSubordinates, cannotReplicateSubordinates...)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted subordinate: %s, lost: %d, trivial: %d, pseudo-gtid: %d",
		promotedSubordinate.Key.DisplayString(), len(lostSubordinates), len(equalSubordinates), len(aheadSubordinates)), Details: promotedSubordinate.Key})
}

// RegroupSubordinates attempts to pick a subordinate of a given instance and make it ensubordinate its siblings, efficiently,
// using pseudo-gtid if necessary
func (this *HttpAPI) RegroupSubordinatesPseudoGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	lostSubordinates, equalSubordinates, aheadSubordinates, cannotReplicateSubordinates, promotedSubordinate, err := inst.RegroupSubordinatesPseudoGTID(&instanceKey, false, nil, nil)
	lostSubordinates = append(lostSubordinates, cannotReplicateSubordinates...)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted subordinate: %s, lost: %d, trivial: %d, pseudo-gtid: %d",
		promotedSubordinate.Key.DisplayString(), len(lostSubordinates), len(equalSubordinates), len(aheadSubordinates)), Details: promotedSubordinate.Key})
}

// RegroupSubordinatesGTID attempts to pick a subordinate of a given instance and make it ensubordinate its siblings, efficiently, using GTID
func (this *HttpAPI) RegroupSubordinatesGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	lostSubordinates, movedSubordinates, cannotReplicateSubordinates, promotedSubordinate, err := inst.RegroupSubordinatesGTID(&instanceKey, false, nil)
	lostSubordinates = append(lostSubordinates, cannotReplicateSubordinates...)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted subordinate: %s, lost: %d, moved: %d",
		promotedSubordinate.Key.DisplayString(), len(lostSubordinates), len(movedSubordinates)), Details: promotedSubordinate.Key})
}

// RegroupSubordinatesBinlogServers attempts to pick a subordinate of a given instance and make it ensubordinate its siblings, efficiently, using GTID
func (this *HttpAPI) RegroupSubordinatesBinlogServers(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	_, promotedBinlogServer, err := inst.RegroupSubordinatesBinlogServers(&instanceKey, false)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted binlog server: %s",
		promotedBinlogServer.Key.DisplayString()), Details: promotedBinlogServer.Key})
}

// MakeMain attempts to make the given instance a main, and match its siblings to be its subordinates
func (this *HttpAPI) MakeMain(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MakeMain(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v now made main", instanceKey), Details: instance})
}

// MakeLocalMain attempts to make the given instance a local main: take over its main by
// enslaving its siblings and replicating from its grandparent.
func (this *HttpAPI) MakeLocalMain(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MakeLocalMain(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v now made local main", instanceKey), Details: instance})
}

// SkipQuery skips a single query on a failed replication instance
func (this *HttpAPI) SkipQuery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SkipQuery(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Query skipped on %+v", instance.Key), Details: instance})
}

// StartSubordinate starts replication on given instance
func (this *HttpAPI) StartSubordinate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StartSubordinate(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Subordinate started: %+v", instance.Key), Details: instance})
}

// RestartSubordinate stops & starts replication on given instance
func (this *HttpAPI) RestartSubordinate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.RestartSubordinate(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Subordinate restarted: %+v", instance.Key), Details: instance})
}

// StopSubordinate stops replication on given instance
func (this *HttpAPI) StopSubordinate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StopSubordinate(&instanceKey)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Subordinate stopped: %+v", instance.Key), Details: instance})
}

// StopSubordinateNicely stops replication on given instance, such that sql thead is aligned with IO thread
func (this *HttpAPI) StopSubordinateNicely(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StopSubordinateNicely(&instanceKey, 0)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Subordinate stopped nicely: %+v", instance.Key), Details: instance})
}

// MainEquivalent provides (possibly empty) list of main coordinates equivalent to the given ones
func (this *HttpAPI) MainEquivalent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	coordinates, err := this.getBinlogCoordinates(params["logFile"], params["logPos"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instanceCoordinates := &inst.InstanceBinlogCoordinates{Key: instanceKey, Coordinates: coordinates}

	equivalentCoordinates, err := inst.GetEquivalentMainCoordinates(instanceCoordinates)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Found %+v equivalent coordinates", len(equivalentCoordinates)), Details: equivalentCoordinates})
}

// SetReadOnly sets the global read_only variable
func (this *HttpAPI) SetReadOnly(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SetReadOnly(&instanceKey, true)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Server set as read-only", Details: instance})
}

// SetWriteable clear the global read_only variable
func (this *HttpAPI) SetWriteable(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SetReadOnly(&instanceKey, false)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Server set as writeable", Details: instance})
}

// KillQuery kills a query running on a server
func (this *HttpAPI) KillQuery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	processId, err := strconv.ParseInt(params["process"], 10, 0)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.KillQuery(&instanceKey, processId)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Query killed on : %+v", instance.Key), Details: instance})
}

// Cluster provides list of instances in given cluster
func (this *HttpAPI) Cluster(params martini.Params, r render.Render, req *http.Request) {
	instances, err := inst.ReadClusterInstances(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

// ClusterByAlias provides list of instances in given cluster
func (this *HttpAPI) ClusterByAlias(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := inst.GetClusterByAlias(params["clusterAlias"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = clusterName
	this.Cluster(params, r, req)
}

// ClusterByInstance provides list of instances in cluster an instance belongs to
func (this *HttpAPI) ClusterByInstance(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}

	params["clusterName"] = instance.ClusterName
	this.Cluster(params, r, req)
}

// ClusterInfo provides details of a given cluster
func (this *HttpAPI) ClusterInfo(params martini.Params, r render.Render, req *http.Request) {
	clusterInfo, err := inst.ReadClusterInfo(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, clusterInfo)
}

// Cluster provides list of instances in given cluster
func (this *HttpAPI) ClusterInfoByAlias(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := inst.GetClusterByAlias(params["clusterAlias"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = clusterName
	this.ClusterInfo(params, r, req)
}

// ClusterOSCSubordinates returns heuristic list of OSC subordinates
func (this *HttpAPI) ClusterOSCSubordinates(params martini.Params, r render.Render, req *http.Request) {
	instances, err := inst.GetClusterOSCSubordinates(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

// SetClusterAlias will change an alias for a given clustername
func (this *HttpAPI) SetClusterAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName := params["clusterName"]
	alias := req.URL.Query().Get("alias")

	err := inst.SetClusterAlias(clusterName, alias)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Cluster %s now has alias '%s'", clusterName, alias)})
}

// Clusters provides list of known clusters
func (this *HttpAPI) Clusters(params martini.Params, r render.Render, req *http.Request) {
	clusterNames, err := inst.ReadClusters()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, clusterNames)
}

// ClustersInfo provides list of known clusters, along with some added metadata per cluster
func (this *HttpAPI) ClustersInfo(params martini.Params, r render.Render, req *http.Request) {
	clustersInfo, err := inst.ReadClustersInfo("")

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, clustersInfo)
}

// Search provides list of instances matching given search param via various criteria.
func (this *HttpAPI) Search(params martini.Params, r render.Render, req *http.Request) {
	searchString := params["searchString"]
	if searchString == "" {
		searchString = req.URL.Query().Get("s")
	}
	instances, err := inst.SearchInstances(searchString)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

// Problems provides list of instances with known problems
func (this *HttpAPI) Problems(params martini.Params, r render.Render, req *http.Request) {
	clusterName := params["clusterName"]
	instances, err := inst.ReadProblemInstances(clusterName)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

// Audit provides list of audit entries by given page number
func (this *HttpAPI) Audit(params martini.Params, r render.Render, req *http.Request) {
	page, err := strconv.Atoi(params["page"])
	if err != nil || page < 0 {
		page = 0
	}
	var auditedInstanceKey *inst.InstanceKey
	if instanceKey, err := this.getInstanceKey(params["host"], params["port"]); err == nil {
		auditedInstanceKey = &instanceKey
	}

	audits, err := inst.ReadRecentAudit(auditedInstanceKey, page)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, audits)
}

// LongQueries lists queries running for a long time, on all instances, optionally filtered by
// arbitrary text
func (this *HttpAPI) LongQueries(params martini.Params, r render.Render, req *http.Request) {
	longQueries, err := inst.ReadLongRunningProcesses(params["filter"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, longQueries)
}

// HostnameResolveCache shows content of in-memory hostname cache
func (this *HttpAPI) HostnameResolveCache(params martini.Params, r render.Render, req *http.Request) {
	content, err := inst.HostnameResolveCache()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Cache retrieved", Details: content})
}

// ResetHostnameResolveCache clears in-memory hostname resovle cache
func (this *HttpAPI) ResetHostnameResolveCache(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := inst.ResetHostnameResolveCache()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Hostname cache cleared"})
}

// DeregisterHostnameUnresolve deregisters the unresolve name used previously
func (this *HttpAPI) DeregisterHostnameUnresolve(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	var instanceKey *inst.InstanceKey
	if instKey, err := this.getInstanceKey(params["host"], params["port"]); err == nil {
		instanceKey = &instKey
	}
	if err := inst.DeregisterHostnameUnresolve(instanceKey); err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Hostname deregister unresolve completed"})
}

// RegisterHostnameUnresolve registers the unresolve name to use
func (this *HttpAPI) RegisterHostnameUnresolve(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	var instanceKey *inst.InstanceKey
	if instKey, err := this.getInstanceKey(params["host"], params["port"]); err == nil {
		instanceKey = &instKey
	}
	if err := inst.RegisterHostnameUnresolve(instanceKey, params["virtualname"]); err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Hostname register unresolve completed"})
}

// SubmitPoolInstances (re-)applies the list of hostnames for a given pool
func (this *HttpAPI) SubmitPoolInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	pool := params["pool"]
	instances := req.URL.Query().Get("instances")

	err := inst.ApplyPoolInstances(pool, instances)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Applied %s pool instances", pool)})
}

// SubmitPoolHostnames (re-)applies the list of hostnames for a given pool
func (this *HttpAPI) ReadClusterPoolInstancesMap(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName := params["clusterName"]
	pool := params["pool"]

	poolInstancesMap, err := inst.ReadClusterPoolInstancesMap(clusterName, pool)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Read pool instances for cluster %s", clusterName), Details: poolInstancesMap})
}

// GetHeuristicClusterPoolInstances returns instances belonging to a cluster's pool
func (this *HttpAPI) GetHeuristicClusterPoolInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := inst.ReadClusterNameByAlias(params["clusterName"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	pool := params["pool"]

	instances, err := inst.GetHeuristicClusterPoolInstances(clusterName, pool)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Heuristic pool instances for cluster %s", clusterName), Details: instances})
}

// GetHeuristicClusterPoolInstances returns instances belonging to a cluster's pool
func (this *HttpAPI) GetHeuristicClusterPoolInstancesLag(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := inst.ReadClusterNameByAlias(params["clusterName"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	pool := params["pool"]

	lag, err := inst.GetHeuristicClusterPoolInstancesLag(clusterName, pool)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Heuristic pool lag for cluster %s", clusterName), Details: lag})
}

// ReloadClusterAlias clears in-memory hostname resovle cache
func (this *HttpAPI) ReloadClusterAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	r.JSON(200, &APIResponse{Code: ERROR, Message: "This API call has been retired"})
}

// Agents provides complete list of registered agents (See https://github.com/github/orchestrator-agent)
func (this *HttpAPI) Agents(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	agents, err := agent.ReadAgents()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, agents)
}

// Agent returns complete information of a given agent
func (this *HttpAPI) Agent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	agent, err := agent.GetAgent(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, agent)
}

// AgentUnmount instructs an agent to unmount the designated mount point
func (this *HttpAPI) AgentUnmount(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.Unmount(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentMountLV instructs an agent to mount a given volume on the designated mount point
func (this *HttpAPI) AgentMountLV(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.MountLV(params["host"], req.URL.Query().Get("lv"))

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentCreateSnapshot instructs an agent to create a new snapshot. Agent's DIY implementation.
func (this *HttpAPI) AgentCreateSnapshot(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.CreateSnapshot(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentRemoveLV instructs an agent to remove a logical volume
func (this *HttpAPI) AgentRemoveLV(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.RemoveLV(params["host"], req.URL.Query().Get("lv"))

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentMySQLStop stops MySQL service on agent
func (this *HttpAPI) AgentMySQLStop(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.MySQLStop(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentMySQLStart starts MySQL service on agent
func (this *HttpAPI) AgentMySQLStart(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.MySQLStart(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

func (this *HttpAPI) AgentCustomCommand(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.CustomCommand(params["host"], params["command"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentSeed completely seeds a host with another host's snapshots. This is a complex operation
// governed by orchestrator and executed by the two agents involved.
func (this *HttpAPI) AgentSeed(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.Seed(params["targetHost"], params["sourceHost"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentActiveSeeds lists active seeds and their state
func (this *HttpAPI) AgentActiveSeeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.ReadActiveSeedsForHost(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentRecentSeeds lists recent seeds of a given agent
func (this *HttpAPI) AgentRecentSeeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.ReadRecentCompletedSeedsForHost(params["host"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentSeedDetails provides details of a given seed
func (this *HttpAPI) AgentSeedDetails(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
	output, err := agent.AgentSeedDetails(seedId)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AgentSeedStates returns the breakdown of states (steps) of a given seed
func (this *HttpAPI) AgentSeedStates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
	output, err := agent.ReadSeedStates(seedId)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// BulkPromotionRules returns a list of the known promotion rules for each instance
func (this *HttpAPI) BulkPromotionRules(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	promotionRules, err := inst.BulkReadCandidateDatabaseInstance()
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, promotionRules)
}

// BulkInstances returns a list of all known instances
func (this *HttpAPI) BulkInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	instances, err := inst.BulkReadInstance()
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, instances)
}

// Seeds retruns all recent seeds
func (this *HttpAPI) Seeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	output, err := agent.ReadRecentSeeds()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, output)
}

// AbortSeed instructs agents to abort an active seed
func (this *HttpAPI) AbortSeed(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	if !config.Config.ServeAgentsHttp {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Agents not served"})
		return
	}

	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
	err = agent.AbortSeed(seedId)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, err == nil)
}

// Headers is a self-test call which returns HTTP headers
func (this *HttpAPI) Headers(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(200, req.Header)
}

// Health performs a self test
func (this *HttpAPI) Health(params martini.Params, r render.Render, req *http.Request) {
	health, err := process.HealthTest()
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Application node is unhealthy %+v", err), Details: health})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Application node is healthy"), Details: health})

}

// LBCheck returns a constant respnse, and this can be used by load balancers that expect a given string.
func (this *HttpAPI) LBCheck(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(200, "OK")
}

// A configurable endpoint that can be for regular status checks or whatever.  While similar to
// Health() this returns 500 on failure.  This will prevent issues for those that have come to
// expect a 200
// It might be a good idea to deprecate the current Health() behavior and roll this in at some
// point
func (this *HttpAPI) StatusCheck(params martini.Params, r render.Render, req *http.Request) {
	// SimpleHealthTest just checks to see if we can connect to the database.  Lighter weight if you intend to call it a lot
	var health *process.HealthStatus
	var err error
	if config.Config.StatusSimpleHealth {
		health, err = process.SimpleHealthTest()
	} else {
		health, err = process.HealthTest()
	}
	if err != nil {
		r.JSON(500, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Application node is unhealthy %+v", err), Details: health})
		return
	}
	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Application node is healthy"), Details: health})
}

// GrabElection forcibly grabs leadership. Use with care!!
func (this *HttpAPI) GrabElection(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := process.GrabElection()
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Unable to grab election: %+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Node elected as leader")})
}

// Reelect causes re-elections for an active node
func (this *HttpAPI) Reelect(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := process.Reelect()
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Unable to re-elect: %+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Set re-elections")})

}

// ReloadConfiguration reloads confiug settings (not all of which will apply after change)
func (this *HttpAPI) ReloadConfiguration(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	config.Reload()
	inst.AuditOperation("reload-configuration", nil, "Triggered via API")

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Config reloaded")})

}

// ReplicationAnalysis retuens list of issues
func (this *HttpAPI) ReplicationAnalysis(params martini.Params, r render.Render, req *http.Request) {
	analysis, err := inst.GetReplicationAnalysis(params["clusterName"], true, false)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot get analysis: %+v", err)})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Analysis"), Details: analysis})
}

// RecoverLite attempts recovery on a given instance, without executing external processes
func (this *HttpAPI) RecoverLite(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	params["skipProcesses"] = "true"
	this.Recover(params, r, req, user)
}

// Recover attempts recovery on a given instance
func (this *HttpAPI) Recover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	var candidateKey *inst.InstanceKey
	if key, err := this.getInstanceKey(params["candidateHost"], params["candidatePort"]); err == nil {
		candidateKey = &key
	}

	skipProcesses := (req.URL.Query().Get("skipProcesses") == "true") || (params["skipProcesses"] == "true")
	recoveryAttempted, _, err := logic.CheckAndRecover(&instanceKey, candidateKey, skipProcesses)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if recoveryAttempted {
		r.JSON(200, &APIResponse{Code: OK, Message: "Action taken", Details: instanceKey})
	} else {
		r.JSON(200, &APIResponse{Code: OK, Message: "No action taken", Details: instanceKey})
	}
}

// Registers promotion preference for given instance
func (this *HttpAPI) RegisterCandidate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	promotionRule, err := inst.ParseCandidatePromotionRule(params["promotionRule"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	err = inst.RegisterCandidateInstance(&instanceKey, promotionRule)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(200, &APIResponse{Code: OK, Message: "Registered candidate", Details: instanceKey})
}

// AutomatedRecoveryFilters retuens list of clusters which are configured with automated recovery
func (this *HttpAPI) AutomatedRecoveryFilters(params martini.Params, r render.Render, req *http.Request) {
	automatedRecoveryMap := make(map[string]interface{})
	automatedRecoveryMap["RecoverMainClusterFilters"] = config.Config.RecoverMainClusterFilters
	automatedRecoveryMap["RecoverIntermediateMainClusterFilters"] = config.Config.RecoverIntermediateMainClusterFilters
	automatedRecoveryMap["RecoveryIgnoreHostnameFilters"] = config.Config.RecoveryIgnoreHostnameFilters

	r.JSON(200, &APIResponse{Code: OK, Message: fmt.Sprintf("Automated recovery configuration details"), Details: automatedRecoveryMap})
}

// AuditFailureDetection provides list of topology_failure_detection entries
func (this *HttpAPI) AuditFailureDetection(params martini.Params, r render.Render, req *http.Request) {

	var audits []logic.TopologyRecovery
	var err error

	if detectionId, derr := strconv.ParseInt(params["id"], 10, 0); derr == nil && detectionId > 0 {
		audits, err = logic.ReadFailureDetection(detectionId)
	} else {
		page, derr := strconv.Atoi(params["page"])
		if derr != nil || page < 0 {
			page = 0
		}
		audits, err = logic.ReadRecentFailureDetections(page)
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, audits)
}

// ReadReplicationAnalysisChangelog lists instances and their analysis changelog
func (this *HttpAPI) ReadReplicationAnalysisChangelog(params martini.Params, r render.Render, req *http.Request) {
	changelogs, err := inst.ReadReplicationAnalysisChangelog()

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, changelogs)
}

// AuditRecovery provides list of topology-recovery entries
func (this *HttpAPI) AuditRecovery(params martini.Params, r render.Render, req *http.Request) {
	var audits []logic.TopologyRecovery
	var err error

	if recoveryId, derr := strconv.ParseInt(params["id"], 10, 0); derr == nil && recoveryId > 0 {
		audits, err = logic.ReadRecovery(recoveryId)
	} else {
		page, derr := strconv.Atoi(params["page"])
		if derr != nil || page < 0 {
			page = 0
		}
		unacknowledgedOnly := (req.URL.Query().Get("unacknowledged") == "true")
		audits, err = logic.ReadRecentRecoveries(params["clusterName"], unacknowledgedOnly, page)
	}

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, audits)
}

// ActiveClusterRecovery returns recoveries in-progress for a given cluster
func (this *HttpAPI) ActiveClusterRecovery(params martini.Params, r render.Render, req *http.Request) {
	recoveries, err := logic.ReadActiveClusterRecovery(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, recoveries)
}

// RecentlyActiveClusterRecovery returns recoveries in-progress for a given cluster
func (this *HttpAPI) RecentlyActiveClusterRecovery(params martini.Params, r render.Render, req *http.Request) {
	recoveries, err := logic.ReadRecentlyActiveClusterRecovery(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, recoveries)
}

// RecentlyActiveClusterRecovery returns recoveries in-progress for a given cluster
func (this *HttpAPI) RecentlyActiveInstanceRecovery(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	recoveries, err := logic.ReadRecentlyActiveInstanceRecovery(&instanceKey)

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, recoveries)
}

// ClusterInfo provides details of a given cluster
func (this *HttpAPI) AcknowledgeClusterRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	clusterName := params["clusterName"]
	if params["clusterAlias"] != "" {
		var err error
		clusterName, err = inst.GetClusterByAlias(params["clusterAlias"])
		if err != nil {
			r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
			return
		}
	}

	comment := req.URL.Query().Get("comment")
	if comment == "" {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = inst.GetMaintenanceOwner()
	}
	countAcnowledgedRecoveries, err := logic.AcknowledgeClusterRecoveries(clusterName, userId, comment)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, countAcnowledgedRecoveries)
}

// ClusterInfo provides details of a given cluster
func (this *HttpAPI) AcknowledgeInstanceRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	comment := req.URL.Query().Get("comment")
	if comment == "" {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = inst.GetMaintenanceOwner()
	}
	countAcnowledgedRecoveries, err := logic.AcknowledgeInstanceRecoveries(&instanceKey, userId, comment)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, countAcnowledgedRecoveries)
}

// ClusterInfo provides details of a given cluster
func (this *HttpAPI) AcknowledgeRecovery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	recoveryId, err := strconv.ParseInt(params["recoveryId"], 10, 0)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	comment := req.URL.Query().Get("comment")
	if comment == "" {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = inst.GetMaintenanceOwner()
	}
	countAcnowledgedRecoveries, err := logic.AcknowledgeRecovery(recoveryId, userId, comment)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, countAcnowledgedRecoveries)
}

// BlockedRecoveries reads list of currently blocked recoveries, optionally filtered by cluster name
func (this *HttpAPI) BlockedRecoveries(params martini.Params, r render.Render, req *http.Request) {
	blockedRecoveries, err := logic.ReadBlockedRecoveries(params["clusterName"])

	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(200, blockedRecoveries)
}

// RegisterRequests makes for the de-facto list of known API calls
func (this *HttpAPI) RegisterRequests(m *martini.ClassicMartini) {
	// Smart relocation:
	m.Get(this.URLPrefix+"/api/relocate/:host/:port/:belowHost/:belowPort", this.RelocateBelow)
	m.Get(this.URLPrefix+"/api/relocate-below/:host/:port/:belowHost/:belowPort", this.RelocateBelow)
	m.Get(this.URLPrefix+"/api/relocate-subordinates/:host/:port/:belowHost/:belowPort", this.RelocateSubordinates)
	m.Get(this.URLPrefix+"/api/regroup-subordinates/:host/:port", this.RegroupSubordinates)

	// Classic file:pos relocation:
	m.Get(this.URLPrefix+"/api/move-up/:host/:port", this.MoveUp)
	m.Get(this.URLPrefix+"/api/move-up-subordinates/:host/:port", this.MoveUpSubordinates)
	m.Get(this.URLPrefix+"/api/move-below/:host/:port/:siblingHost/:siblingPort", this.MoveBelow)
	m.Get(this.URLPrefix+"/api/move-equivalent/:host/:port/:belowHost/:belowPort", this.MoveEquivalent)
	m.Get(this.URLPrefix+"/api/repoint-subordinates/:host/:port", this.RepointSubordinates)
	m.Get(this.URLPrefix+"/api/make-co-main/:host/:port", this.MakeCoMain)
	m.Get(this.URLPrefix+"/api/ensubordinate-siblings/:host/:port", this.EnsubordinateSiblings)
	m.Get(this.URLPrefix+"/api/ensubordinate-main/:host/:port", this.EnsubordinateMain)
	m.Get(this.URLPrefix+"/api/main-equivalent/:host/:port/:logFile/:logPos", this.MainEquivalent)

	// Binlog server relocation:
	m.Get(this.URLPrefix+"/api/regroup-subordinates-bls/:host/:port", this.RegroupSubordinatesBinlogServers)

	// GTID relocation:
	m.Get(this.URLPrefix+"/api/move-below-gtid/:host/:port/:belowHost/:belowPort", this.MoveBelowGTID)
	m.Get(this.URLPrefix+"/api/move-subordinates-gtid/:host/:port/:belowHost/:belowPort", this.MoveSubordinatesGTID)
	m.Get(this.URLPrefix+"/api/regroup-subordinates-gtid/:host/:port", this.RegroupSubordinatesGTID)

	// Pseudo-GTID relocation:
	m.Get(this.URLPrefix+"/api/match/:host/:port/:belowHost/:belowPort", this.MatchBelow)
	m.Get(this.URLPrefix+"/api/match-below/:host/:port/:belowHost/:belowPort", this.MatchBelow)
	m.Get(this.URLPrefix+"/api/match-up/:host/:port", this.MatchUp)
	m.Get(this.URLPrefix+"/api/match-subordinates/:host/:port/:belowHost/:belowPort", this.MultiMatchSubordinates)
	m.Get(this.URLPrefix+"/api/multi-match-subordinates/:host/:port/:belowHost/:belowPort", this.MultiMatchSubordinates)
	m.Get(this.URLPrefix+"/api/match-up-subordinates/:host/:port", this.MatchUpSubordinates)
	m.Get(this.URLPrefix+"/api/regroup-subordinates-pgtid/:host/:port", this.RegroupSubordinatesPseudoGTID)
	// Legacy, need to revisit:
	m.Get(this.URLPrefix+"/api/make-main/:host/:port", this.MakeMain)
	m.Get(this.URLPrefix+"/api/make-local-main/:host/:port", this.MakeLocalMain)

	// Replication, general:
	m.Get(this.URLPrefix+"/api/enable-gtid/:host/:port", this.EnableGTID)
	m.Get(this.URLPrefix+"/api/disable-gtid/:host/:port", this.DisableGTID)
	m.Get(this.URLPrefix+"/api/skip-query/:host/:port", this.SkipQuery)
	m.Get(this.URLPrefix+"/api/start-subordinate/:host/:port", this.StartSubordinate)
	m.Get(this.URLPrefix+"/api/restart-subordinate/:host/:port", this.RestartSubordinate)
	m.Get(this.URLPrefix+"/api/stop-subordinate/:host/:port", this.StopSubordinate)
	m.Get(this.URLPrefix+"/api/stop-subordinate-nice/:host/:port", this.StopSubordinateNicely)
	m.Get(this.URLPrefix+"/api/reset-subordinate/:host/:port", this.ResetSubordinate)
	m.Get(this.URLPrefix+"/api/detach-subordinate/:host/:port", this.DetachSubordinate)
	m.Get(this.URLPrefix+"/api/reattach-subordinate/:host/:port", this.ReattachSubordinate)
	m.Get(this.URLPrefix+"/api/reattach-subordinate-main-host/:host/:port", this.ReattachSubordinateMainHost)

	// Instance:
	m.Get(this.URLPrefix+"/api/set-read-only/:host/:port", this.SetReadOnly)
	m.Get(this.URLPrefix+"/api/set-writeable/:host/:port", this.SetWriteable)
	m.Get(this.URLPrefix+"/api/kill-query/:host/:port/:process", this.KillQuery)

	// Binary logs:
	m.Get(this.URLPrefix+"/api/last-pseudo-gtid/:host/:port", this.LastPseudoGTID)

	// Pools:
	m.Get(this.URLPrefix+"/api/submit-pool-instances/:pool", this.SubmitPoolInstances)
	m.Get(this.URLPrefix+"/api/cluster-pool-instances/:clusterName", this.ReadClusterPoolInstancesMap)
	m.Get(this.URLPrefix+"/api/cluster-pool-instances/:clusterName/:pool", this.ReadClusterPoolInstancesMap)
	m.Get(this.URLPrefix+"/api/heuristic-cluster-pool-instances/:clusterName", this.GetHeuristicClusterPoolInstances)
	m.Get(this.URLPrefix+"/api/heuristic-cluster-pool-instances/:clusterName/:pool", this.GetHeuristicClusterPoolInstances)
	m.Get(this.URLPrefix+"/api/heuristic-cluster-pool-lag/:clusterName", this.GetHeuristicClusterPoolInstancesLag)
	m.Get(this.URLPrefix+"/api/heuristic-cluster-pool-lag/:clusterName/:pool", this.GetHeuristicClusterPoolInstancesLag)

	// Information:
	m.Get(this.URLPrefix+"/api/search/:searchString", this.Search)
	m.Get(this.URLPrefix+"/api/search", this.Search)

	// Cluster
	m.Get(this.URLPrefix+"/api/cluster/:clusterName", this.Cluster)
	m.Get(this.URLPrefix+"/api/cluster/alias/:clusterAlias", this.ClusterByAlias)
	m.Get(this.URLPrefix+"/api/cluster/instance/:host/:port", this.ClusterByInstance)
	m.Get(this.URLPrefix+"/api/cluster-info/:clusterName", this.ClusterInfo)
	m.Get(this.URLPrefix+"/api/cluster-info/alias/:clusterAlias", this.ClusterInfoByAlias)
	m.Get(this.URLPrefix+"/api/cluster-osc-subordinates/:clusterName", this.ClusterOSCSubordinates)
	m.Get(this.URLPrefix+"/api/set-cluster-alias/:clusterName", this.SetClusterAlias)
	m.Get(this.URLPrefix+"/api/clusters", this.Clusters)
	m.Get(this.URLPrefix+"/api/clusters-info", this.ClustersInfo)

	// Instance management:
	m.Get(this.URLPrefix+"/api/instance/:host/:port", this.Instance)
	m.Get(this.URLPrefix+"/api/discover/:host/:port", this.Discover)
	m.Get(this.URLPrefix+"/api/refresh/:host/:port", this.Refresh)
	m.Get(this.URLPrefix+"/api/forget/:host/:port", this.Forget)
	m.Get(this.URLPrefix+"/api/begin-maintenance/:host/:port/:owner/:reason", this.BeginMaintenance)
	m.Get(this.URLPrefix+"/api/end-maintenance/:host/:port", this.EndMaintenanceByInstanceKey)
	m.Get(this.URLPrefix+"/api/end-maintenance/:maintenanceKey", this.EndMaintenance)
	m.Get(this.URLPrefix+"/api/begin-downtime/:host/:port/:owner/:reason", this.BeginDowntime)
	m.Get(this.URLPrefix+"/api/begin-downtime/:host/:port/:owner/:reason/:duration", this.BeginDowntime)
	m.Get(this.URLPrefix+"/api/end-downtime/:host/:port", this.EndDowntime)

	// Recovery:
	m.Get(this.URLPrefix+"/api/replication-analysis", this.ReplicationAnalysis)
	m.Get(this.URLPrefix+"/api/replication-analysis/:clusterName", this.ReplicationAnalysis)
	m.Get(this.URLPrefix+"/api/recover/:host/:port", this.Recover)
	m.Get(this.URLPrefix+"/api/recover/:host/:port/:candidateHost/:candidatePort", this.Recover)
	m.Get(this.URLPrefix+"/api/recover-lite/:host/:port", this.RecoverLite)
	m.Get(this.URLPrefix+"/api/recover-lite/:host/:port/:candidateHost/:candidatePort", this.RecoverLite)
	m.Get(this.URLPrefix+"/api/register-candidate/:host/:port/:promotionRule", this.RegisterCandidate)
	m.Get(this.URLPrefix+"/api/automated-recovery-filters", this.AutomatedRecoveryFilters)
	m.Get(this.URLPrefix+"/api/audit-failure-detection", this.AuditFailureDetection)
	m.Get(this.URLPrefix+"/api/audit-failure-detection/:page", this.AuditFailureDetection)
	m.Get(this.URLPrefix+"/api/audit-failure-detection/id/:id", this.AuditFailureDetection)
	m.Get(this.URLPrefix+"/api/replication-analysis-changelog", this.ReadReplicationAnalysisChangelog)
	m.Get(this.URLPrefix+"/api/audit-recovery", this.AuditRecovery)
	m.Get(this.URLPrefix+"/api/audit-recovery/:page", this.AuditRecovery)
	m.Get(this.URLPrefix+"/api/audit-recovery/id/:id", this.AuditRecovery)
	m.Get(this.URLPrefix+"/api/audit-recovery/cluster/:clusterName", this.AuditRecovery)
	m.Get(this.URLPrefix+"/api/audit-recovery/cluster/:clusterName/:page", this.AuditRecovery)
	m.Get(this.URLPrefix+"/api/active-cluster-recovery/:clusterName", this.ActiveClusterRecovery)
	m.Get(this.URLPrefix+"/api/recently-active-cluster-recovery/:clusterName", this.RecentlyActiveClusterRecovery)
	m.Get(this.URLPrefix+"/api/recently-active-instance-recovery/:host/:port", this.RecentlyActiveInstanceRecovery)
	m.Get(this.URLPrefix+"/api/ack-recovery/cluster/:clusterName", this.AcknowledgeClusterRecoveries)
	m.Get(this.URLPrefix+"/api/ack-recovery/cluster/alias/:clusterAlias", this.AcknowledgeClusterRecoveries)
	m.Get(this.URLPrefix+"/api/ack-recovery/instance/:host/:port", this.AcknowledgeInstanceRecoveries)
	m.Get(this.URLPrefix+"/api/ack-recovery/:recoveryId", this.AcknowledgeRecovery)
	m.Get(this.URLPrefix+"/api/blocked-recoveries", this.BlockedRecoveries)
	m.Get(this.URLPrefix+"/api/blocked-recoveries/cluster/:clusterName", this.BlockedRecoveries)

	// General
	m.Get(this.URLPrefix+"/api/problems", this.Problems)
	m.Get(this.URLPrefix+"/api/problems/:clusterName", this.Problems)
	m.Get(this.URLPrefix+"/api/long-queries", this.LongQueries)
	m.Get(this.URLPrefix+"/api/long-queries/:filter", this.LongQueries)
	m.Get(this.URLPrefix+"/api/audit", this.Audit)
	m.Get(this.URLPrefix+"/api/audit/:page", this.Audit)
	m.Get(this.URLPrefix+"/api/audit/instance/:host/:port", this.Audit)
	m.Get(this.URLPrefix+"/api/audit/instance/:host/:port/:page", this.Audit)
	m.Get(this.URLPrefix+"/api/resolve/:host/:port", this.Resolve)

	// Meta
	m.Get(this.URLPrefix+"/api/maintenance", this.Maintenance)
	m.Get(this.URLPrefix+"/api/headers", this.Headers)
	m.Get(this.URLPrefix+"/api/health", this.Health)
	m.Get(this.URLPrefix+"/api/lb-check", this.LBCheck)
	m.Get(this.URLPrefix+"/api/grab-election", this.GrabElection)
	m.Get(this.URLPrefix+"/api/reelect", this.Reelect)
	m.Get(this.URLPrefix+"/api/reload-configuration", this.ReloadConfiguration)
	m.Get(this.URLPrefix+"/api/reload-cluster-alias", this.ReloadClusterAlias)
	m.Get(this.URLPrefix+"/api/hostname-resolve-cache", this.HostnameResolveCache)
	m.Get(this.URLPrefix+"/api/reset-hostname-resolve-cache", this.ResetHostnameResolveCache)
	m.Get(this.URLPrefix+"/api/deregister-hostname-unresolve/:host/:port", this.DeregisterHostnameUnresolve)
	m.Get(this.URLPrefix+"/api/register-hostname-unresolve/:host/:port/:virtualname", this.RegisterHostnameUnresolve)

	// Bulk access to information
	m.Get("/api/bulk-instances", this.BulkInstances)
	m.Get("/api/bulk-promotion-rules", this.BulkPromotionRules)

	// Agents
	m.Get(this.URLPrefix+"/api/agents", this.Agents)
	m.Get(this.URLPrefix+"/api/agent/:host", this.Agent)
	m.Get(this.URLPrefix+"/api/agent-umount/:host", this.AgentUnmount)
	m.Get(this.URLPrefix+"/api/agent-mount/:host", this.AgentMountLV)
	m.Get(this.URLPrefix+"/api/agent-create-snapshot/:host", this.AgentCreateSnapshot)
	m.Get(this.URLPrefix+"/api/agent-removelv/:host", this.AgentRemoveLV)
	m.Get(this.URLPrefix+"/api/agent-mysql-stop/:host", this.AgentMySQLStop)
	m.Get(this.URLPrefix+"/api/agent-mysql-start/:host", this.AgentMySQLStart)
	m.Get(this.URLPrefix+"/api/agent-seed/:targetHost/:sourceHost", this.AgentSeed)
	m.Get(this.URLPrefix+"/api/agent-active-seeds/:host", this.AgentActiveSeeds)
	m.Get(this.URLPrefix+"/api/agent-recent-seeds/:host", this.AgentRecentSeeds)
	m.Get(this.URLPrefix+"/api/agent-seed-details/:seedId", this.AgentSeedDetails)
	m.Get(this.URLPrefix+"/api/agent-seed-states/:seedId", this.AgentSeedStates)
	m.Get(this.URLPrefix+"/api/agent-abort-seed/:seedId", this.AbortSeed)
	m.Get(this.URLPrefix+"/api/agent-custom-command/:host/:command", this.AgentCustomCommand)
	m.Get(this.URLPrefix+"/api/seeds", this.Seeds)

	// Configurable status check endpoint
	m.Get(config.Config.StatusEndpoint, this.StatusCheck)
}
