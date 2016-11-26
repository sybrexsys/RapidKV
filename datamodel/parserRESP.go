package datamodel

import (
	"bufio"
	"strconv"
)

func skipCR(b []byte, offset *int) error {
	if len(b) <= *offset+1 || b[*offset] != '\r' || b[*offset+1] != '\n' {
		return ParseError("invalid tocken was found")
	}
	*offset += 2
	return nil
}

// LoadRESPObj parses byte slice and returns data object
func LoadRESPObj(b []byte) (CustomDataType, error) {
	if len(b) == 0 {
		return nil, ParseError("empty data")
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
		return nil, ParseError("end of string was not found")
	case '-':
		for i := *offset + 1; i < lenb-1; i++ {
			if b[i] == '\r' && b[i+1] == '\n' {
				k := *offset
				*offset = i + 2
				return CreateError(string(b[k+1 : i])), nil
			}
		}
		return nil, ParseError("end of error was not found: " + string(b[*offset+1:]))
	case '*':
		*offset++
		err := getNumberLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltInteger {
			return nil, ParseError("invalid tocken was found")
		}
		if lex.intValue < -1 {
			return nil, ParseError("invalid array length was found")
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
			return nil, ParseError("invalid tocken was found")
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
		if lex.lexType != ltInteger || lex.intValue < -1 {
			return nil, ParseError("invalid tocken was found")
		}
		err = skipCR(b, offset)
		if err != nil {
			return nil, err
		}
		if lex.intValue == -1 {
			return CreateNull(), nil
		}
		if lenb <= *offset+lex.intValue {
			return nil, ParseError("string data was not found")
		}
		tmp := CreateString(string(b[*offset : *offset+lex.intValue]))
		*offset += lex.intValue
		err = skipCR(b, offset)
		if err != nil {
			return nil, err
		}
		return tmp, nil
	}
	return nil, ParseError("invalid tocken was found")
}

// LoadRespFromIO parses input reader and returns data object
func LoadRespFromIO(reader *bufio.Reader, isFirstLayout bool) (CustomDataType, bool, error) {
	s, err := reader.ReadString(10)
	if err != nil {
		return nil, false, err
	}
	switch s[0] {
	case ':':
		tmp, err := strconv.Atoi(s[1 : len(s)-2])
		if err != nil {
			return nil, false, err
		}
		return CreateInt(tmp), false, nil
	case '-':
		return CreateError(s[1 : len(s)-2]), false, nil
	case '+':
		return CreateSimpleString(s[1 : len(s)-2]), false, nil
	case '*':
		tmp, err := strconv.Atoi(s[1 : len(s)-2])
		if err != nil {
			return nil, false, err
		}
		if tmp == -1 {
			tmp := CreateArray(0)
			tmp.(*dataArray).isNull = true
			return tmp, false, nil
		}
		if tmp == 0 {
			tmp := CreateArray(0)
			return tmp, false, nil
		}
		if tmp < -1 {
			return nil, false, ParseError("invalid array size was detected")
		}
		array := CreateArray(tmp)
		for i := 0; i < tmp; i++ {
			data, _, err := LoadRespFromIO(reader, false)
			if err != nil {
				return nil, false, err
			}
			array.Add(data)
		}
		return array, false, nil
	case '$':
		tmp, err := strconv.Atoi(s[1 : len(s)-2])
		if err != nil {
			return nil, false, err
		}
		if tmp == -1 {
			return CreateNull(), false, nil
		}
		if tmp < -1 {
			return nil, false, ParseError("invalid size of string")
		}
		if tmp == 0 {
			return CreateString(""), false, nil
		}
		str := make([]byte, tmp)
		buf := str[:]
		for {
			n, err := reader.Read(buf)
			if err != nil {
				return nil, false, err
			}
			if n == len(buf) {
				break
			}
			buf = buf[n:]
		}
		s, err := reader.ReadString(10)
		if err != nil {
			return nil, false, err
		}
		if s != "\r\n" {
			return nil, false, ParseError("unknown token was found")
		}
		return CreateString(string(str)), false, nil
	}
	if !isFirstLayout {
		return nil, false, ParseError("unknown token was detected")
	}
	obj, err := processLazyString(s)
	return obj, true, err
}

func processLazyString(str string) (CustomDataType, error) {

	data := []byte(str)
	lenb := len(data)
	lex := &lexeme{}
	array := CreateArray(10)
mainloop:
	for offset := 0; offset < lenb; offset++ {
		if data[offset] < 33 {
			continue
		}
		switch data[offset] {
		case '"':
			tmpoff := offset
			err := getStringLexeme(data, &offset, lex)
			if err == nil {
				array.Add(CreateString(lex.str))
				continue mainloop
			}
			offset = tmpoff
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '.':
			tmpoff := offset
			err := getNumberLexeme(data, &offset, lex)
			if err == nil && lex.lexType == ltInteger {
				array.Add(CreateInt(lex.intValue))
				continue mainloop
			}
			offset = tmpoff
		}
		i := 0
		for i = offset; i < lenb; i++ {
			if data[i] < 33 {
				break
			}
		}
		str := string(data[offset:i])
		offset = i
		if str == "nil" {
			array.Add(CreateNull())
		} else {
			array.Add(CreateString(str))
		}
	}
	return array, nil
}
