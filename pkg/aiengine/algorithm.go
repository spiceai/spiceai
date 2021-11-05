package aiengine

type LearningAlgorithm struct {
	Id   string
	Name string
}

var (
	algorithms = map[string]*LearningAlgorithm{
		"vpg": {
			Id:   "vpg",
			Name: "Vanilla Policy Gradient",
		},
		"dql": {
			Id:   "dql",
			Name: "Deep Q-Learning",
		},
	}
)

func GetAlgorithm(id string) *LearningAlgorithm {
	return algorithms[id]
}
