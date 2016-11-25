package datamodel

import (
	"errors"
)

func skipCR(b []byte, offset *int) error {
	if len(b) <= *offset+1 || b[*offset] != '\r' || b[*offset+1] != '\n' {
		return errors.New("invalid tocken was found")
	}
	*offset += 2
	return nil
}

func LoadRESPObj(b []byte) (CustomDataType, error) {
	if len(b) == 0 {
		return nil, errors.New("empty data")
	}
	offset := 0
	lex := lexeme{}
	return loadRESPObj(b, &offset, &lex)
}

func loadRESPObj(b []byte, offset *int, lex *lexeme) (CustomDataType, error) {
	lenb := len(b)
	switch b[*offset] {
	case '+':
		for i := *offset + 1; i < lenb-1; i++ {
			if b[i] == '\r' && b[i+1] == '\n' {
				k := *offset
				*offset = i + 2
				return CreateSimpleString(string(b[k+1 : i])), nil
			}
		}
		return nil, errors.New("end of string was not found")
	case '-':
		for i := *offset + 1; i < lenb-1; i++ {
			if b[i] == '\r' && b[i+1] == '\n' {
				k := *offset
				*offset = i + 2
				return CreateError(string(b[k+1 : i])), nil
			}
		}
		return nil, errors.New("end of error was not found: " + string(b[*offset+1:]))
	case '*':
		*offset++
		err := getNumberLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltInteger {
			return nil, errors.New("invalid tocken was found")
		}
		if lex.intValue < -1 {
			return nil, errors.New("invalid array length was found")
		}
		err = skipCR(b, offset)
		if err != nil {
			return nil, err
		}
		l := lex.intValue
		if l == -1 {
			tmp := CreateArray(0)
			tmp.(*dataArray).isNull = true
			return tmp, nil
		}
		tmp := CreateArray(l)
		for i := 0; i < l; i++ {
			a, err := loadRESPObj(b, offset, lex)
			if err != nil {
				return nil, err
			}
			tmp.Add(a)
		}
		return tmp, nil
	case ':':
		*offset++
		err := getNumberLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltInteger {
			return nil, errors.New("invalid tocken was found")
		}
		err = skipCR(b, offset)
		if err != nil {
			return nil, err
		}
		return CreateInt(lex.intValue), nil
	case '$':
		*offset++
		err := getNumberLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltInteger {
			return nil, errors.New("invalid tocken was found")
		}
		err = skipCR(b, offset)
		if err != nil {
			return nil, err
		}
		if lex.intValue == -1 {
			return CreateNull(), nil
		}
		if lenb <= *offset+lex.intValue {
			return nil, errors.New("string data was not found")
		}
		tmp := CreateString(string(b[*offset : *offset+lex.intValue]))
		*offset += lex.intValue
		err = skipCR(b, offset)
		if err != nil {
			return nil, err
		}
		return tmp, nil
	}
	return nil, errors.New("invalid tocken was found")
}
