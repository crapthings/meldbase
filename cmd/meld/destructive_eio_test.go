package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDestructiveEIOSeedCreatesVerifiedReusableFixture(t *testing.T) {
	database := filepath.Join(t.TempDir(), "eio.meld")
	var output strings.Builder
	if err := runDestructiveEIOSeed([]string{"--database", database}, &output, &output); err != nil {
		t.Fatal(err)
	}
	var result destructiveEIOSeedResult
	if err := json.Unmarshal([]byte(output.String()), &result); err != nil || !result.Passed || result.CommitSequence != 16 ||
		result.ReusablePages == 0 || result.Reclaimable == 0 || !qualificationHexDigest(result.DatabaseSHA256) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	verified, err := verifyDestructiveEIODatabase(database)
	if err != nil || !verified.PersistentFreeSpace || !verified.FreeSpaceValid {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}
}

func TestValidateBlkdebugEIOConfigRequiresWriteOnlyUniqueSectors(t *testing.T) {
	valid := []byte("[inject-error]\nevent = \"write_aio\"\niotype = \"write\"\nerrno = \"5\"\nsector = \"10\"\nonce = \"on\"\n\n" +
		"[inject-error]\nevent = \"write_aio\"\niotype = \"write\"\nerrno = \"5\"\nsector = \"11\"\nonce = \"on\"\n")
	sectors, err := validateBlkdebugEIOConfig(valid)
	if err != nil || len(sectors) != 2 {
		t.Fatalf("sectors=%v err=%v", sectors, err)
	}
	for _, mutation := range [][]byte{
		[]byte(strings.Replace(string(valid), `iotype = "write"`, `iotype = "read"`, 1)),
		[]byte(strings.Replace(string(valid), `sector = "11"`, `sector = "10"`, 1)),
		[]byte(strings.Replace(string(valid), `once = "on"`, `once = "off"`, 1)),
	} {
		if _, err := validateBlkdebugEIOConfig(mutation); err == nil {
			t.Fatal("unsafe blkdebug configuration was accepted")
		}
	}
}

func TestValidateQMPEIOProofAcceptsAsyncBlockErrorBetweenCommands(t *testing.T) {
	now := time.Now().UTC()
	target, config := "/control/target.img", "/control/eio.conf"
	inventory := `[{"inserted":{"ro":false,"drv":"raw","file":"blkdebug:/control/eio.conf:/control/target.img","image":{"filename":"blkdebug:/control/eio.conf:/control/target.img"},"cache":{"direct":true,"no-flush":false}}}]`
	exchanges := []destructiveQMPExchange{
		qmpTestExchange("receive", now, `{"QMP":{"version":{}}}`),
		qmpTestExchange("send", now.Add(time.Millisecond), `{"execute":"qmp_capabilities","id":"eio-capabilities"}`),
		qmpTestExchange("receive", now.Add(2*time.Millisecond), `{"return":{},"id":"eio-capabilities"}`),
		qmpTestExchange("send", now.Add(3*time.Millisecond), `{"execute":"query-block","id":"eio-query-block-before"}`),
		qmpTestExchange("receive", now.Add(4*time.Millisecond), `{"return":`+inventory+`,"id":"eio-query-block-before"}`),
		qmpTestExchange("receive", now.Add(5*time.Millisecond), `{"event":"BLOCK_IO_ERROR","data":{"operation":"write","action":"report","nospace":false,"reason":"Input/output error"}}`),
		qmpTestExchange("send", now.Add(6*time.Millisecond), `{"execute":"query-block","id":"eio-query-block-after"}`),
		qmpTestExchange("receive", now.Add(7*time.Millisecond), `{"return":`+inventory+`,"id":"eio-query-block-after"}`),
	}
	proof := destructiveQMPEIOProof{
		SchemaVersion: destructiveQMPEIOProofSchema, TargetImage: target, BlkdebugConfig: config,
		BlkdebugSHA256: strings.Repeat("11", 32), InjectedSectorCount: 2, ResultSHA256: strings.Repeat("22", 32),
		StartedAt: now, FinishedAt: now.Add(8 * time.Millisecond), Exchanges: exchanges,
	}
	if err := validateQMPEIOProofStructure(proof); err != nil {
		t.Fatal(err)
	}
	proof.Exchanges[5] = qmpTestExchange("receive", now.Add(5*time.Millisecond), `{"event":"BLOCK_IO_ERROR","data":{"operation":"read","action":"report","reason":"Input/output error"}}`)
	if err := validateQMPEIOProofStructure(proof); err == nil {
		t.Fatal("read error was accepted as write EIO proof")
	}
}
