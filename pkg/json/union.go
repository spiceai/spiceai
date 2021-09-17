package json

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Unmarshals from json a union data type that can contain either an int64, string or float64
func UnmarshalUnion(data []byte, pi **int64, ps **string, pf **float64) error {
	*pi = nil
	*ps = nil
	*pf = nil

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
		f, err := v.Float64()
		if err == nil {
			*pf = &f
			return nil
		}
		return err
	case string:
		*ps = &v
		return nil
	}
	return errors.New("cannot unmarshal union")
}

// Marshals to json a union data type that can contain either an int64, string or float64
func MarshalUnion(pi *int64, ps *string, pf *float64) ([]byte, error) {
	if pi != nil {
		return json.Marshal(*pi)
	}
	if ps != nil {
		return json.Marshal(*ps)
	}
	if pf != nil {
		return json.Marshal(*pf)
	}
	return nil, errors.New("union must not be null")
}
