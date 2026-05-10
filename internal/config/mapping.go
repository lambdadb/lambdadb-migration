package config

type MappingConfig struct {
	Target        MappingTarget                  `json:"target" yaml:"target"`
	Vectors       map[string]VectorMapping       `json:"vectors,omitempty" yaml:"vectors,omitempty"`
	SparseVectors map[string]SparseVectorMapping `json:"sparseVectors,omitempty" yaml:"sparseVectors,omitempty"`
	Payload       PayloadMapping                 `json:"payload" yaml:"payload"`
	IDs           IDMapping                      `json:"ids" yaml:"ids"`
}

type MappingTarget struct {
	Collection       string `json:"collection" yaml:"collection"`
	CreateCollection bool   `json:"createCollection" yaml:"createCollection"`
}

type VectorMapping struct {
	TargetField string `json:"targetField" yaml:"targetField"`
	Dimensions  int64  `json:"dimensions" yaml:"dimensions"`
	Similarity  string `json:"similarity" yaml:"similarity"`
}

type SparseVectorMapping struct {
	TargetField string `json:"targetField" yaml:"targetField"`
}

type PayloadMapping struct {
	Mode         string                    `json:"mode" yaml:"mode"`
	Rename       map[string]string         `json:"rename,omitempty" yaml:"rename,omitempty"`
	IndexConfigs map[string]map[string]any `json:"indexConfigs,omitempty" yaml:"indexConfigs,omitempty"`
}

type IDMapping struct {
	TargetField    string `json:"targetField" yaml:"targetField"`
	CopyOriginalTo string `json:"copyOriginalTo,omitempty" yaml:"copyOriginalTo,omitempty"`
}
