package time

import (
	spice_json "github.com/spiceai/spiceai/pkg/json"
)

type Time struct {
	Integer *int64
	String  *string
}

func (x *Time) UnmarshalJSON(data []byte) error {
	err := spice_json.UnmarshalUnion(data, &x.Integer, &x.String)
	if err != nil {
		return err
	}
	return nil
}

func (x *Time) MarshalJSON() ([]byte, error) {
	return spice_json.MarshalUnion(x.Integer, x.String)
}
