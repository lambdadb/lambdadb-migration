package main

import (
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestInspectionCLIHelper(t *testing.T) {
	if os.Getenv("CONNECTION_INSPECTION_HELPER") != "1" {
		return
	}
	os.Args = []string{"connection-worker", "-inspect-state", "-state-dir", os.Getenv("CONNECTION_INSPECTION_DIRECTORY")}
	if os.Getenv("CONNECTION_INSPECTION_INITIALIZE") == "1" {
		os.Args = append(os.Args, "-initialize")
	}
	flag.CommandLine = flag.NewFlagSet("inspection-helper", flag.ExitOnError)
	main()
}

func TestInspectionCLIRefusesInitializationAndDoesNotCreateFiles(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, initialize := range []bool{false, true} {
		dir := t.TempDir()
		command := exec.Command(binary, "-test.run=^TestInspectionCLIHelper$")
		command.Env = append(os.Environ(), "CONNECTION_INSPECTION_HELPER=1", "CONNECTION_INSPECTION_DIRECTORY="+dir)
		want := "journal inspection requires"
		if initialize {
			command.Env = append(command.Env, "CONNECTION_INSPECTION_INITIALIZE=1")
			want = "inspection cannot initialize"
		}
		out, err := command.CombinedOutput()
		if err == nil || !strings.Contains(string(out), want) {
			t.Fatalf("unexpected inspection error: %s", out)
		}
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 0 {
			t.Fatal("failed inspection created state")
		}
	}
}

func TestManagedWorkerRequiresS3BeforeOpeningLocalState(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	command := exec.Command(binary, "-test.run=^TestInspectionCLIHelper$")
	command.Env = append(os.Environ(), "CONNECTION_INSPECTION_HELPER=1", "CONNECTION_INSPECTION_DIRECTORY="+dir, "CONNECTION_REQUIRE_S3_STATE=1")
	out, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "managed execution requires S3 recovery state") {
		t.Fatal("managed execution permitted local-only recovery")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatal("managed fallback touched local state")
	}
}
