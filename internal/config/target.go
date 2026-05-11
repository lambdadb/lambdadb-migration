package config

type LambdaDBConfig struct {
	BaseURL     string `help:"LambdaDB API base URL for your LambdaDB Cloud region." env:"LAMBDADB_BASE_URL" required:"true"`
	ProjectName string `help:"LambdaDB project name." env:"LAMBDADB_PROJECT_NAME" required:"true"`
	APIKey      string `help:"LambdaDB project API key." env:"LAMBDADB_PROJECT_API_KEY" required:"true"`
	Collection  string `help:"Target LambdaDB collection name." required:"true"`
}
