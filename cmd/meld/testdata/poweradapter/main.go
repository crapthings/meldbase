package main

import (
	"encoding/json"
	"os"
	"time"
)

type request struct {
	SchemaVersion        uint32    `json:"schemaVersion"`
	ControllerRunID      string    `json:"controllerRunId"`
	TrialID              string    `json:"trialId"`
	Method               string    `json:"method"`
	TargetIdentitySHA256 string    `json:"targetIdentitySha256"`
	RequestedAt          time.Time `json:"requestedAt"`
}

func main() {
	var input request
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		panic(err)
	}
	response := struct {
		SchemaVersion          uint32    `json:"schemaVersion"`
		ControllerRunID        string    `json:"controllerRunId"`
		TrialID                string    `json:"trialId"`
		Method                 string    `json:"method"`
		OperationID            string    `json:"operationId"`
		TargetIdentitySHA256   string    `json:"targetIdentitySha256"`
		AcceptedAt             time.Time `json:"acceptedAt"`
		PowerLostAt            time.Time `json:"powerLostAt"`
		PowerRestoredAt        time.Time `json:"powerRestoredAt"`
		HardwareEvidenceSHA256 string    `json:"hardwareEvidenceSha256"`
		HardwareEvidenceBase64 string    `json:"hardwareEvidenceBase64"`
		Success                bool      `json:"success"`
	}{
		SchemaVersion: input.SchemaVersion, ControllerRunID: input.ControllerRunID,
		TrialID: input.TrialID, Method: input.Method, OperationID: "test-operation",
		TargetIdentitySHA256: input.TargetIdentitySHA256,
		AcceptedAt:           input.RequestedAt, PowerLostAt: input.RequestedAt.Add(time.Nanosecond),
		PowerRestoredAt: input.RequestedAt.Add(2 * time.Nanosecond), HardwareEvidenceSHA256: "a2946c9328f4418fe234d3b4b70dce9d49bca7efae371bd5d4ef284cdde84290", HardwareEvidenceBase64: "dGVzdCBoYXJkd2FyZSBldmlkZW5jZQ==", Success: true,
	}
	if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
		panic(err)
	}
}
