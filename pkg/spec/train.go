package spec

type TrainSpec struct {
	EpochTime int64  `json:"epoch_time,omitempty"`
	Episodes  int    `json:"number_episodes"`
	FlightId  string `json:"flight"`
	Goal      string `json:"goal,omitempty"`
}
