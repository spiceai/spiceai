package json

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Unmarshals from json a union data type that can contain either an int64, string or float64
func UnmarshalUnion(data []byte, pi **int64, ps **string, pf **float64) error {
	if pi != nil {
		*pi = nil
	}
	if ps != nil {
		*ps = nil
	}
	if pf != nil {
		*pf = nil
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return err
	}

	switch v := tok.(type) {
	case json.Number:
		if pi != nil {
			i, err := v.Int64()
			if err == nil {
				*pi = &i
				return nil
			}
		}
		if pf != nil {
			f, err := v.Float64()
			if err == nil {
				*pf = &f
				return nil
			}
			return errors.New("unparsable number")
		}
		return errors.New("union does not contain number")
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
