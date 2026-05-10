package cmd

import (
	"fmt"
	"os"

	"github.com/alecthomas/kong"
)

type Globals struct {
	Debug   bool             `help:"Enable debug output."`
	Trace   bool             `help:"Enable trace output."`
	Version kong.VersionFlag `name:"version" help:"Print version and exit."`
}

type CLI struct {
	Globals

	Inventory InventoryCmd     `cmd:"" help:"Inspect a source and generate a LambdaDB migration mapping."`
	Qdrant    MigrateQdrantCmd `cmd:"" help:"Migrate data from Qdrant to LambdaDB."`
}

func Execute(version, commit string) {
	cli := CLI{}
	ctx := kong.Parse(&cli,
		kong.Name("lambdadb-migration"),
		kong.Description("Migrate vector/search data sources into LambdaDB."),
		kong.Vars{
			"version": fmt.Sprintf("%s (%s)", version, commit),
		},
	)

	if err := ctx.Run(&cli.Globals); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		ctx.Exit(1)
	}
}
