package config

type LambdaDBConfig struct {
	BaseURL     string `help:"LambdaDB API base URL." default:"https://api.lambdadb.ai"`
	ProjectName string `help:"LambdaDB project name." required:"true"`
	APIKey      string `help:"LambdaDB project API key." env:"LAMBDADB_PROJECT_API_KEY" required:"true"`
	Collection  string `help:"Target LambdaDB collection name." required:"true"`
}
