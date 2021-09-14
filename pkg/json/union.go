package json

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Unmarshals from json a union data type that can contain either an int64 or a string
func UnmarshalUnion(data []byte, pi **int64, ps **string) error {
	*pi = nil
	*ps = nil

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return err
	}

	switch v := tok.(type) {
	case json.Number:
		i, err := v.Int64()
		if err == nil {
			*pi = &i
			return nil
		}
		return err
	case string:
		*ps = &v
		return nil
	}
	return errors.New("cannot unmarshal Time")
}

// Marshals to json a union data type that can contain either an int64 or a string
func MarshalUnion(pi *int64, ps *string) ([]byte, error) {
	if pi != nil {
		return json.Marshal(*pi)
	}
	if ps != nil {
		return json.Marshal(*ps)
	}
	return nil, errors.New("time must not be null")
}
