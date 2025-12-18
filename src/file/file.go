package file

import (
	"encoding/json"
	"os"
)

type Serializer interface {
	Serialize(v any, path string) error
	Deserialize(v any, path string) error
}

type JSONSerializer struct{}

func (s *JSONSerializer) Serialize(v any, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(v)
}

func (s *JSONSerializer) Deserialize(v any, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(v)
}
