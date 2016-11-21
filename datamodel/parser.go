package datamodel

import (
	"errors"
)

const (
	ltInteger = iota
	ltReal
	ltEOF
	ltBoolean
	ltNull
	ltSmCommand
	ltString
)

const (
	smOpenArray = iota
	smCloseArray
	smOpenDictionary
	smCloseDictionary
	smComma
	smColon
)

type lexeme struct {
	lexType   int
	str       string
	intValue  int
	realValue float64
}

type back int

func (back) getLength() int                     { return 0 }
func (back) writeToBytes(b []byte) (int, error) { return 0, nil }

func getStringLexeme(b []byte, offset *int, lex *lexeme) error {
	*offset++
	lenb := len(b)
	length := 0
	for {
		if *offset+length == lenb {
			return errors.New("unterminate string lexeme")
		}
		ch := b[*offset+length]
		if ch == '"' {
			break
		}
		length++
		if ch != '\\' {
			continue
		}
		if *offset+length == lenb {
			return errors.New("unterminate string lexeme")
		}
		ch = b[*offset+length]
		if ch == 'r' || ch == 'n' || ch == 't' || ch == 'f' || ch == 'b' || ch == '\\' || ch == '"' {
			continue
		}
		if ch != 'u' {
			return errors.New("invalid token")
		}
		if *offset+length+4 >= lenb {
			return errors.New("unterminate string lexeme")
		}

	}
	//arr := make([]byte, length)
	for {
		ch := b[*offset+length]
		if ch == '"' {
			break
		}
		length++
		if ch != '\\' {
			continue
		}
		ch = b[*offset+length]
		if ch == 'r' || ch == 'n' || ch == 't' || ch == 'f' || ch == 'b' {
			continue
		}
		if ch == 'u' {

		}
		return errors.New("invalid token")
	}
	return nil
}

func getNumberLexeme(b []byte, offset *int, lex *lexeme) error {
	return nil
}

func getLexeme(b []byte, offset *int, lex *lexeme) error {
	lenb := len(b)
	var ch byte
	var buf [5]byte
	for {
		if *offset >= lenb {
			lex.lexType = ltEOF
			return nil
		}
		ch = b[*offset]
		if ch == '\t' || ch == '\r' || ch == '\n' || ch == ' ' {
			*offset++
			continue
		}
		break
	}
	switch ch {
	case '{':
		lex.intValue = smOpenDictionary
		lex.lexType = ltSmCommand
		*offset++
		return nil
	case '}':
		lex.intValue = smCloseDictionary
		lex.lexType = ltSmCommand
		*offset++
		return nil
	case '[':
		lex.intValue = smOpenArray
		lex.lexType = ltSmCommand
		*offset++
		return nil
	case ']':
		lex.intValue = smCloseArray
		lex.lexType = ltSmCommand
		*offset++
		return nil
	case ':':
		lex.intValue = smColon
		lex.lexType = ltSmCommand
		*offset++
		return nil
	case ',':
		lex.intValue = smComma
		lex.lexType = ltSmCommand
		*offset++
		return nil
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '.':
		return getNumberLexeme(b, offset, lex)
	case '"':
		return getStringLexeme(b, offset, lex)
	default:
		i := 0
		for {
			if i == 5 {
				return errors.New("unknow token was found")
			}
			buf[i] = ch
			*offset++
			if *offset >= lenb {
				break
			}
			ch = b[*offset]
			if ch == '\t' || ch == '\r' || ch == '\n' || ch == ' ' {
				*offset++
				break
			}
			if ch == '}' || ch == ']' || ch == ',' {
				break
			}
			i++
		}
		str := string(buf[:i+1])
		if str == "null" {
			lex.lexType = ltNull
			return nil
		}
		if str == "true" {
			lex.lexType = ltBoolean
			lex.intValue = 1
			return nil
		}
		if str == "false" {
			lex.lexType = ltBoolean
			lex.intValue = 0
			return nil
		}
	}
	return errors.New("unknow token was found")
}

func processArray(b []byte, offset *int, lex *lexeme) (*dataArray, error) {
	tmp := CreateArray(10)
	for {
		obj, err := ParseObj(b, offset, lex)
		if err != nil {
			return nil, err
		}
		v, ok := obj.(back)
		if ok {
			if v == smCloseArray {
				return tmp, nil
			}
			return nil, errors.New("invalid lexeme not found")
		}
		tmp.Add(obj)
		err = getLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltSmCommand {
			return nil, errors.New("invalid lexeme not found")
		}
		if lex.intValue == smCloseArray {
			return tmp, nil
		}
		if lex.intValue != smComma {
			return nil, errors.New("invalid lexeme was found")
		}
	}
}

func processDictionary(b []byte, offset *int, lex *lexeme) (*dataDictionary, error) {
	tmp := CreateDictionary(10)
	for {
		err := getLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType == ltSmCommand && lex.intValue == smCloseDictionary {
			return tmp, nil
		}
		if lex.lexType != ltString {
			return nil, errors.New("string lexeme not found")
		}
		key := lex.str
		err = getLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltSmCommand || lex.intValue != smColon {
			return nil, errors.New("string lexeme not found")
		}
		obj, err := ParseObj(b, offset, lex)
		if err != nil {
			return nil, err
		}
		tmp.Add(key, obj)
		err = getLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltSmCommand {
			return nil, errors.New("string lexeme not found")
		}
		if lex.intValue == smCloseDictionary {
			return tmp, nil
		}
		if lex.intValue != smComma {
			return nil, errors.New("invalid lexeme was found")
		}
	}
}

func ParseObj(b []byte, offset *int, lex *lexeme) (CustomDataType, error) {
	err := getLexeme(b, offset, lex)
	if err != nil {
		return nil, err
	}
	switch lex.lexType {
	case ltInteger:
		return CreateInt(lex.intValue), nil
	case ltBoolean:
		return CreateBool(lex.intValue != 0), nil
	case ltNull:
		return CreateNull(), nil
	case ltString:
		return CreateString(lex.str), nil
	case ltEOF:
		return nil, errors.New("EOF was found")
	case ltSmCommand:
		switch lex.intValue {
		case smOpenArray:
			return processArray(b, offset, lex)
		case smOpenDictionary:
			return processDictionary(b, offset, lex)
		case smCloseArray:
			return back(smCloseArray), nil
		case smCloseDictionary:
			return back(smCloseDictionary), nil
		}
	}
	return nil, errors.New("unknow lexeme")
}
