package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	pineconesource "github.com/lambdadb/lambdadb-migration/internal/source/pinecone"
	qdrantsource "github.com/lambdadb/lambdadb-migration/internal/source/qdrant"
)

type InventoryCmd struct {
	Qdrant   InventoryQdrantCmd   `cmd:"" help:"Inspect a Qdrant collection."`
	Pinecone InventoryPineconeCmd `cmd:"" help:"Inspect a Pinecone Serverless index."`
}

type InventoryQdrantCmd struct {
	Qdrant config.QdrantConfig `embed:"" prefix:"qdrant."`
	Output string              `help:"Output path for generated mapping. Use '-' for stdout. .yaml/.yml outputs use YAML; other outputs use JSON." default:"-"`
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
		Inventory any                  `json:"inventory" yaml:"inventory"`
		Mapping   config.MappingConfig `json:"mapping" yaml:"mapping"`
	}{
		Inventory: inv,
		Mapping:   mapping,
	}

	data, err := marshalOutput(c.Output, out)
	if err != nil {
		return fmt.Errorf("encode inventory: %w", err)
	}

	if c.Output == "-" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if err := os.WriteFile(c.Output, data, 0o644); err != nil {
		return fmt.Errorf("write inventory output: %w", err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s inventory mapping to %s\n", outputFormatName(c.Output), c.Output)
	return nil
}

type InventoryPineconeCmd struct {
	Pinecone config.PineconeConfig `embed:"" prefix:"pinecone."`
	Output   string                `help:"Output path for generated mapping. Use '-' for stdout. .yaml/.yml outputs use YAML; other outputs use JSON." default:"-"`
}

func (c *InventoryPineconeCmd) Run(globals *Globals) error {
	ctx := context.Background()
	src, err := pineconesource.New(c.Pinecone)
	if err != nil {
		return err
	}
	defer src.Close()

	inv, err := src.Inventory(ctx)
	if err != nil {
		return err
	}

	mapping := config.MappingFromInventory(inv, c.Pinecone.Index)
	out := struct {
		Inventory any                  `json:"inventory" yaml:"inventory"`
		Mapping   config.MappingConfig `json:"mapping" yaml:"mapping"`
	}{
		Inventory: inv,
		Mapping:   mapping,
	}

	data, err := marshalOutput(c.Output, out)
	if err != nil {
		return fmt.Errorf("encode inventory: %w", err)
	}

	if c.Output == "-" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if err := os.WriteFile(c.Output, data, 0o644); err != nil {
		return fmt.Errorf("write inventory output: %w", err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s inventory mapping to %s\n", outputFormatName(c.Output), c.Output)
	return nil
}
