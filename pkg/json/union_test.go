package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Test struct {
	Integer *int64
	String  *string
}

func TestUnion(t *testing.T) {
	t.Run("UnmarshalUnion()", testUnmarshalUnionFunc())
	t.Run("MarshalUnion()", testMarshalUnionFunc())
}

func testUnmarshalUnionFunc() func(*testing.T) {
	return func(t *testing.T) {
		test := &Test{}

		err := UnmarshalUnion([]byte("24"), &test.Integer, &test.String)
		assert.NoError(t, err)

		assert.Equal(t, int64(24), *test.Integer)
		assert.Nil(t, test.String)

		test = &Test{}

		err = UnmarshalUnion([]byte("\"this is a string\""), &test.Integer, &test.String)
		assert.NoError(t, err)

		assert.Equal(t, "this is a string", *test.String)
		assert.Nil(t, test.Integer)
	}
}

func testMarshalUnionFunc() func(*testing.T) {
	return func(t *testing.T) {
		test := &Test{}

		val := int64(24)
		test.Integer = &val

		jsonBytes, err := MarshalUnion(test.Integer, test.String)
		assert.NoError(t, err)
		assert.Equal(t, "24", string(jsonBytes))

		test = &Test{}

		strVal := "this is a string"
		test.String = &strVal

		jsonBytes, err = MarshalUnion(test.Integer, test.String)
		assert.NoError(t, err)
		assert.Equal(t, "\"this is a string\"", string(jsonBytes))
	}
}
