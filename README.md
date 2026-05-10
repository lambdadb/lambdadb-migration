# LambdaDB Migration

CLI tooling for migrating vector databases and search systems into LambdaDB.

The initial implementation targets Qdrant as the first source, then expands toward source parity with Qdrant's migration tool. LambdaDB is the only target.

See [DESIGN.md](DESIGN.md) for the current architecture and implementation plan.

## Mapping Files

Generate an inventory and editable mapping from Qdrant:

```bash
go run . inventory qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --output qdrant-inventory.yaml
```

`inventory qdrant` writes YAML for `.yaml`/`.yml` outputs and JSON otherwise. `--mapping-file` accepts either JSON or YAML, as a direct mapping object or as the wrapped output produced by `inventory qdrant`.
