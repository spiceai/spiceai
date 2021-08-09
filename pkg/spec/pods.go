package spec

type PodInitSpec struct {
	EpochTime   *int64               `json:"epoch_time,omitempty"`
	Period      int64                `json:"period"`
	Interval    int                  `json:"interval"`
	Granularity int                  `json:"granularity"`
	DataSources []DataSourceInitSpec `json:"datasources"`
	Fields      map[string]float64   `json:"fields"`
	Actions     map[string]string    `json:"actions"`
	Laws        []string             `json:"laws"`
}

type PodSpec struct {
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Params      map[string]string `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
	DataSources []DataSourceSpec  `json:"datasources,omitempty" yaml:"datasources,omitempty" mapstructure:"datasources,omitempty"`
	Actions     []PodActionSpec   `json:"actions,omitempty" yaml:"actions,omitempty" mapstructure:"actions,omitempty"`
	Training    *TrainingSpec     `json:"training,omitempty" yaml:"training,omitempty" mapstructure:"training,omitempty"`
}

type PodActionSpec struct {
	Name string  `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Do   *DoSpec `json:"do,omitempty" yaml:"do,omitempty" mapstructure:"do,omitempty"`
}

type DoSpec struct {
	Name string            `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Args map[string]string `json:"args,omitempty" yaml:"args,omitempty" mapstructure:"args,omitempty"`
}

type TrainingSpec struct {
	Goal       string            `json:"goal,omitempty" yaml:"goal,omitempty" mapstructure:"goal,omitempty"`
	RewardInit string            `json:"reward_init,omitempty" yaml:"reward_init,omitempty" mapstructure:"reward_init,omitempty"`
	RewardArgs map[string]string `json:"reward_args,omitempty" yaml:"reward_args,omitempty" mapstructure:"reward_args,omitempty"`
	Rewards    interface{}       `json:"rewards,omitempty" yaml:"rewards,omitempty" mapstructure:"rewards,omitempty"`
}

type RewardSpec struct {
	Reward string `json:"reward,omitempty" yaml:"reward,omitempty" mapstructure:"reward,omitempty"`
	With   string `json:"with,omitempty" yaml:"with,omitempty" mapstructure:"with,omitempty"`
}
