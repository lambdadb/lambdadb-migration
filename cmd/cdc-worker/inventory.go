package main

import (
	"encoding/json"
	"io"
	"os"

	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
)

func maintainInventory(statePath string, inspect bool, manifestPath string, out io.Writer) error {
	var report *postgrescdc.InventoryInspection
	var err error
	if inspect {
		report, err = postgrescdc.InspectInventory(statePath)
	} else {
		file, e := os.Open(manifestPath)
		if e != nil {
			return e
		}
		defer file.Close()
		report, err = postgrescdc.ReconcileInventory(statePath, file)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}
