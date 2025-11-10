package types

import (
	"encoding/binary"
	"fmt"
	"time"
)

// DataType 数据类型
type DataType uint8

const (
	TypeInt DataType = iota
	TypeText
	TypeBoolean
	TypeFloat
	TypeDate
)

func (t DataType) String() string {
	switch t {
	case TypeInt:
		return "INT"
	case TypeText:
		return "TEXT"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeFloat:
		return "FLOAT"
	case TypeDate:
		return "DATE"
	default:
		return "UNKNOWN"
	}
}

// Value 存储任意类型的值
type Value struct {
	Type DataType
	Data interface{} // int64, string, bool, float64, time.Time
}

// NewIntValue 创建整数值
func NewIntValue(v int64) Value {
	return Value{Type: TypeInt, Data: v}
}

// NewTextValue 创建文本值
func NewTextValue(v string) Value {
	return Value{Type: TypeText, Data: v}
}

// NewBooleanValue 创建布尔值
func NewBooleanValue(v bool) Value {
	return Value{Type: TypeBoolean, Data: v}
}

// NewFloatValue 创建浮点值
func NewFloatValue(v float64) Value {
	return Value{Type: TypeFloat, Data: v}
}

// NewDateValue 创建日期值
func NewDateValue(v time.Time) Value {
	return Value{Type: TypeDate, Data: v}
}

// AsInt 获取整数值
func (v Value) AsInt() (int64, error) {
	if v.Type != TypeInt {
		return 0, fmt.Errorf("value is not int, got %s", v.Type)
	}
	return v.Data.(int64), nil
}

// AsText 获取文本值
func (v Value) AsText() (string, error) {
	if v.Type != TypeText {
		return "", fmt.Errorf("value is not text, got %s", v.Type)
	}
	return v.Data.(string), nil
}

// AsBoolean 获取布尔值
func (v Value) AsBoolean() (bool, error) {
	if v.Type != TypeBoolean {
		return false, fmt.Errorf("value is not boolean, got %s", v.Type)
	}
	return v.Data.(bool), nil
}

// AsFloat 获取浮点值
func (v Value) AsFloat() (float64, error) {
	if v.Type != TypeFloat {
		return 0, fmt.Errorf("value is not float, got %s", v.Type)
	}
	return v.Data.(float64), nil
}

// AsDate 获取日期值
func (v Value) AsDate() (time.Time, error) {
	if v.Type != TypeDate {
		return time.Time{}, fmt.Errorf("value is not date, got %s", v.Type)
	}
	return v.Data.(time.Time), nil
}

// Serialize 序列化为字节数组（用于存储）
func (v Value) Serialize() ([]byte, error) {
	buf := make([]byte, 1) // 第一个字节存储类型
	buf[0] = byte(v.Type)

	switch v.Type {
	case TypeInt:
		intVal := v.Data.(int64)
		intBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(intBuf, uint64(intVal))
		buf = append(buf, intBuf...)

	case TypeText:
		textVal := v.Data.(string)
		textBytes := []byte(textVal)
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(textBytes)))
		buf = append(buf, lenBuf...)
		buf = append(buf, textBytes...)

	case TypeBoolean:
		boolVal := v.Data.(bool)
		if boolVal {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}

	case TypeFloat:
		floatVal := v.Data.(float64)
		floatBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(floatBuf, uint64(floatVal))
		buf = append(buf, floatBuf...)

	case TypeDate:
		dateVal := v.Data.(time.Time)
		timestamp := dateVal.Unix()
		dateBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(dateBuf, uint64(timestamp))
		buf = append(buf, dateBuf...)

	default:
		return nil, fmt.Errorf("unsupported type: %s", v.Type)
	}

	return buf, nil
}

// Deserialize 从字节数组反序列化
func Deserialize(data []byte) (Value, int, error) {
	if len(data) < 1 {
		return Value{}, 0, fmt.Errorf("data too short")
	}

	dataType := DataType(data[0])
	offset := 1

	switch dataType {
	case TypeInt:
		if len(data) < offset+8 {
			return Value{}, 0, fmt.Errorf("data too short for int")
		}
		intVal := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		return NewIntValue(intVal), offset + 8, nil

	case TypeText:
		if len(data) < offset+4 {
			return Value{}, 0, fmt.Errorf("data too short for text length")
		}
		textLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		if len(data) < offset+int(textLen) {
			return Value{}, 0, fmt.Errorf("data too short for text content")
		}
		textVal := string(data[offset : offset+int(textLen)])
		return NewTextValue(textVal), offset + int(textLen), nil

	case TypeBoolean:
		if len(data) < offset+1 {
			return Value{}, 0, fmt.Errorf("data too short for boolean")
		}
		boolVal := data[offset] == 1
		return NewBooleanValue(boolVal), offset + 1, nil

	case TypeFloat:
		if len(data) < offset+8 {
			return Value{}, 0, fmt.Errorf("data too short for float")
		}
		floatVal := float64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		return NewFloatValue(floatVal), offset + 8, nil

	case TypeDate:
		if len(data) < offset+8 {
			return Value{}, 0, fmt.Errorf("data too short for date")
		}
		timestamp := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		dateVal := time.Unix(timestamp, 0)
		return NewDateValue(dateVal), offset + 8, nil

	default:
		return Value{}, 0, fmt.Errorf("unsupported type: %d", dataType)
	}
}

// String 返回值的字符串表示
func (v Value) String() string {
	switch v.Type {
	case TypeInt:
		return fmt.Sprintf("%d", v.Data.(int64))
	case TypeText:
		return v.Data.(string)
	case TypeBoolean:
		return fmt.Sprintf("%t", v.Data.(bool))
	case TypeFloat:
		return fmt.Sprintf("%f", v.Data.(float64))
	case TypeDate:
		return v.Data.(time.Time).Format("2006-01-02")
	default:
		return "UNKNOWN"
	}
}
