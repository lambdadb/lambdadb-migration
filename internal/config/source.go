package config

type QdrantConfig struct {
	URL            string `help:"Qdrant URL. Use the gRPC endpoint when possible." default:"http://localhost:6334"`
	APIKey         string `help:"Qdrant API key."`
	Collection     string `help:"Source Qdrant collection name." required:"true"`
	MaxMessageSize int    `help:"Maximum gRPC receive message size in bytes." default:"33554432"`
}

type ElasticsearchConfig struct {
	URL          string   `help:"Elasticsearch endpoint URL." default:"http://localhost:9200"`
	APIKey       string   `help:"Elasticsearch API key. Defaults to ELASTIC_API_KEY when omitted." env:"ELASTIC_API_KEY"`
	Username     string   `help:"Elasticsearch basic auth username."`
	Password     string   `help:"Elasticsearch basic auth password."`
	Index        string   `help:"Source Elasticsearch index name." required:"true"`
	VectorFields []string `help:"Dense vector field names to fetch explicitly. Defaults to dense_vector fields discovered from the index mapping." sep:","`
	PITKeepAlive string   `help:"Elasticsearch point-in-time keep_alive value." default:"5m"`
}
