package config

type PineconeConfig struct {
	APIKey     string `help:"Pinecone API key. Defaults to PINECONE_API_KEY when omitted."`
	Host       string `help:"Pinecone control-plane API host. Defaults to Pinecone's public API host."`
	Index      string `help:"Source Pinecone index name." required:"true"`
	Namespace  string `help:"Source Pinecone namespace. Defaults to Pinecone's default namespace."`
	ListPrefix string `help:"Only migrate Pinecone vector IDs with this prefix."`
}
