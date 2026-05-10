package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	qdrantsource "github.com/lambdadb/lambdadb-migration/internal/source/qdrant"
)

type InventoryCmd struct {
	Qdrant InventoryQdrantCmd `cmd:"" help:"Inspect a Qdrant collection."`
}

type InventoryQdrantCmd struct {
	Qdrant config.QdrantConfig `embed:"" prefix:"qdrant."`
	Output string              `help:"Output path for generated mapping. Use '-' for stdout." default:"-"`
}

func (c *InventoryQdrantCmd) Run(globals *Globals) error {
	ctx := context.Background()
	src, err := qdrantsource.New(c.Qdrant)
	if err != nil {
		return err
	}
	defer src.Close()

	inv, err := src.Inventory(ctx)
	if err != nil {
		return err
	}

	mapping := config.MappingFromInventory(inv, c.Qdrant.Collection)
	out := struct {
		Inventory any                  `json:"inventory"`
		Mapping   config.MappingConfig `json:"mapping"`
	}{
		Inventory: inv,
		Mapping:   mapping,
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("encode inventory: %w", err)
	}
	data = append(data, '\n')

	if c.Output == "-" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if err := os.WriteFile(c.Output, data, 0o644); err != nil {
		return fmt.Errorf("write inventory output: %w", err)
	}
	fmt.Fprintf(os.Stderr, "wrote inventory mapping to %s\n", c.Output)
	return nil
}
