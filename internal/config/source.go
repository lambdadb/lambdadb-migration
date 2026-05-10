package config

type QdrantConfig struct {
	URL            string `help:"Qdrant URL. Use the gRPC endpoint when possible." default:"http://localhost:6334"`
	APIKey         string `help:"Qdrant API key."`
	Collection     string `help:"Source Qdrant collection name." required:"true"`
	MaxMessageSize int    `help:"Maximum gRPC receive message size in bytes." default:"33554432"`
}
