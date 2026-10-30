package connections

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
)

func inspectionFixture(t *testing.T, mode string) (string, *Journal, *postgrescdc.Store) {
	t.Helper()
	dir := t.TempDir()
	selection := protocol.PostgresSelection{Schema: "public", Table: "items", Columns: []string{"pk", "title"}}
	if mode == "continuous" {
		selection.Publication = "ldb_items"
	}
	raw, _ := json.Marshal(selection)
	a := protocol.Approved{Binding: protocol.Binding{ConnectionID: "inspection-connection", Revision: 1,
		Region: "ap-northeast-2", Mode: mode,
		Source:       protocol.Source{Type: "postgresql", CredentialRef: "env:PRIVATE_SOURCE_REFERENCE", Configuration: raw},
		Target:       protocol.Target{Version: 3, URL: "https://target.invalid", Project: "private-project", Collection: "items", CredentialRef: "env:PRIVATE_TARGET_REFERENCE"},
		IndexConfigs: json.RawMessage(`{"title":{"type":"text"}}`)},
		Control: protocol.Command{ProtocolVersion: 1, ConnectionID: "inspection-connection", DesiredRevision: 1,
			DesiredState: "running", AuthorityExpiresAt: time.Now().Add(time.Hour)}}
	fingerprint := protocol.Fingerprint(a.Binding)
	j, err := OpenJournal(filepath.Join(dir, "connection.db"), fingerprint, true)
	if err != nil {
		t.Fatal(err)
	}
	s, err := postgrescdc.OpenNewStore(filepath.Join(dir, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.BindSource(fingerprint, true); err != nil {
		t.Fatal(err)
	}
	columns := []postgrescdc.Column{{Name: "pk", OID: 20, Key: true}, {Name: "title", OID: 25}}
	rows := map[string]postgrescdc.Row{"1": {"pk": json.RawMessage(`1`), "title": json.RawMessage(`"private-source-value"`)}}
	if mode == "continuous" {
		err = s.Save(&postgrescdc.State{Format: 1, Fingerprint: "capture-fingerprint", SystemID: "source-system", LSN: 100, Columns: columns, Rows: rows})
		if err == nil {
			err = s.RecordInitialContinuous(fingerprint, 0)
		}
	} else {
		err = s.SavePreparedCut(postgrescdc.Cut{Format: 1, Fingerprint: fingerprint,
			Snapshot: &postgrescdc.SnapshotData{RelationID: 5, Columns: columns, Rows: rows}})
		if err == nil {
			err = s.PromotePreparedCut(fingerprint, 0)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(workerData{Approved: a, Phase: "running", Ready: true, CreationAttempt: true, TargetCreated: true})
	if err = j.SaveData(data); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close(); _ = s.Close() })
	return dir, j, s
}

func TestInspectionIsReadOnlyRedactedAndNeverAuthorizesResume(t *testing.T) {
	for _, mode := range []string{"continuous", "snapshot"} {
		for _, status := range []string{"prepared", "sending", "rejected"} {
			t.Run(mode+"/"+status, func(t *testing.T) {
				dir, j, source := inspectionFixture(t, mode)
				intent, err := j.Prepare("delete", json.RawMessage(`{"ids":["private-document-id"]}`))
				if err != nil {
					t.Fatal(err)
				}
				if status != "prepared" {
					if err = j.markSending(*intent); err != nil {
						t.Fatal(err)
					}
				}
				if status == "rejected" {
					if err = j.reject(*intent); err != nil {
						t.Fatal(err)
					}
				}
				_ = j.Close()
				_ = source.Close()
				before := map[string][]byte{}
				for _, name := range []string{"connection.db", "source.db"} {
					before[name], err = os.ReadFile(filepath.Join(dir, name))
					if err != nil {
						t.Fatal(err)
					}
				}
				out, err := InspectState(dir)
				if err != nil {
					t.Fatal(err)
				}
				if out.ResumeAuthorized || !out.LocalStateOnly || out.RecoveryRequired != (status != "prepared") || out.Pending.Status != status || out.Source.Rows != 1 || !out.Source.CheckpointPresent {
					t.Fatal("inspection overstated recovery or lost metadata")
				}
				if mode == "continuous" && out.Source.LSN != "0/64" {
					t.Fatal("checkpoint mismatch")
				}
				raw, _ := json.Marshal(out)
				for _, private := range []string{"private-source-value", "private-document-id", "PRIVATE_SOURCE_REFERENCE", "PRIVATE_TARGET_REFERENCE", "private-project", "target.invalid"} {
					if strings.Contains(string(raw), private) {
						t.Fatal("inspection leaked protected content")
					}
				}
				for name, expected := range before {
					after, err := os.ReadFile(filepath.Join(dir, name))
					if err != nil || !bytes.Equal(expected, after) {
						t.Fatal("inspection changed a state file")
					}
				}
			})
		}
	}
}

func TestInspectionRefusesMissingCorruptAndLockedState(t *testing.T) {
	dir := t.TempDir()
	if _, err := InspectState(dir); err == nil {
		t.Fatal("missing state accepted")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("inspection created missing files")
	}
	dir, j, source := inspectionFixture(t, "continuous")
	if _, err := InspectState(dir); err == nil {
		t.Fatal("active journal accepted")
	}
	_ = j.Close()
	if _, err := InspectState(dir); err == nil {
		t.Fatal("active source accepted")
	}
	_ = source.Close()
	if err := os.WriteFile(filepath.Join(dir, "source.db"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectState(dir); err == nil {
		t.Fatal("empty source accepted")
	}
	info, _ := os.Stat(filepath.Join(dir, "source.db"))
	if info.Size() != 0 {
		t.Fatal("empty source initialized")
	}
}

func TestInspectionRejectsMismatchedSourceOwnerAndInvalidObservation(t *testing.T) {
	dir, j, source := inspectionFixture(t, "continuous")
	other, otherJournal, otherSource := inspectionFixture(t, "snapshot")
	_ = otherJournal.Close()
	_ = otherSource.Close()
	_ = source.Close()
	_ = j.Close()
	raw, err := os.ReadFile(filepath.Join(other, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "source.db"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = InspectState(dir); err == nil {
		t.Fatal("mismatched source mode accepted")
	}

	dir, j, source = inspectionFixture(t, "continuous")
	if err = j.UpdateData(func(raw json.RawMessage) (json.RawMessage, error) {
		var d workerData
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, err
		}
		d.Reason = "private-corrupt-error-value"
		return json.Marshal(d)
	}); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	_ = source.Close()
	if _, err = InspectState(dir); err == nil || strings.Contains(err.Error(), "private-corrupt-error-value") {
		t.Fatal("invalid observation was accepted or exposed")
	}
}

func TestInspectionRetainsReportedUncertaintyWithoutPendingIntent(t *testing.T) {
	dir, j, source := inspectionFixture(t, "continuous")
	if err := j.UpdateData(func(raw json.RawMessage) (json.RawMessage, error) {
		var d workerData
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, err
		}
		d.Phase, d.Reason = "action_required", "target_outcome_uncertain"
		return json.Marshal(d)
	}); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	_ = source.Close()
	out, err := InspectState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if out.Pending != nil || !out.RecoveryRequired || out.ResumeAuthorized {
		t.Fatal("missing pending intent erased independently reported uncertainty")
	}
}
