package spec

type PodSpec struct {
	Name       string            `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Params     map[string]string `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
	Time       *TimeSpec         `json:"time,omitempty" yaml:"time,omitempty" mapstructure:"time,omitempty"`
	Dataspaces []DataspaceSpec   `json:"dataspaces,omitempty" yaml:"dataspaces,omitempty" mapstructure:"dataspaces,omitempty"`
	Actions    []PodActionSpec   `json:"actions,omitempty" yaml:"actions,omitempty" mapstructure:"actions,omitempty"`
	Training   *TrainingSpec     `json:"training,omitempty" yaml:"training,omitempty" mapstructure:"training,omitempty"`
}

type TimeSpec struct {
	Categories []string `json:"categories,omitempty" yaml:"categories,omitempty" mapstructure:"categories,omitempty"`
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
	Goal        string            `json:"goal,omitempty" yaml:"goal,omitempty" mapstructure:"goal,omitempty"`
	Loggers []string 		 `json:"loggers,omitempty" yaml:"loggers,omitempty" mapstructure:"loggers,omitempty"`
	RewardFuncs string            `json:"reward_funcs,omitempty" yaml:"reward_funcs,omitempty" mapstructure:"reward_funcs,omitempty"`
	RewardInit  string            `json:"reward_init,omitempty" yaml:"reward_init,omitempty" mapstructure:"reward_init,omitempty"`
	RewardArgs  map[string]string `json:"reward_args,omitempty" yaml:"reward_args,omitempty" mapstructure:"reward_args,omitempty"`
	Rewards     interface{}       `json:"rewards,omitempty" yaml:"rewards,omitempty" mapstructure:"rewards,omitempty"`
}

type RewardSpec struct {
	Reward string `json:"reward,omitempty" yaml:"reward,omitempty" mapstructure:"reward,omitempty"`
	With   string `json:"with,omitempty" yaml:"with,omitempty" mapstructure:"with,omitempty"`
}
