package util

import (
	"bytes"
	"encoding/json"
)

func InterfaceToStruct(m interface{}, s interface{}) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	err = d.Decode(s)
	if err != nil {
		return err
	}
	return nil
}