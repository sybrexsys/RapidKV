package datamodel

import (
	"errors"
	//"fmt"
	"strconv"
	"unicode/utf8"
)

const (
	ltInteger = iota
	ltReal
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

type eof struct{}

func (eof) Error() string { return "" }

func calcStringSize(b []byte, offset int) (int, error) {
	lenb := len(b)
	length := 0
	i := offset
	for ; i < lenb; i++ {
		ch := b[i]
		if ch == '"' {
			break
		}
		length++
		if ch < 0x1f {
			return -1, errors.New("invalid token was found")
		}
		if ch != '\\' {
			continue
		}
		i++
		if i == lenb {
			return -1, errors.New("unterminate string lexeme was found")
		}
		ch = b[i]
		if ch == 'r' || ch == 'n' || ch == 't' || ch == 'f' || ch == 'b' || ch == '\\' || ch == '"' || ch == '/' {
			continue
		}
		if ch != 'u' {
			return -1, errors.New("invalid token was found")
		}
		length += 2
		i += 4
		if i >= lenb {
			return -1, errors.New("unterminate string lexeme was found")
		}
	}
	if i == lenb {
		return -1, errors.New("unterminate string lexeme was found")
	}
	return length, nil

}

func getRune(b []byte, offset int) (rune, error) {
	i := uint16(0)
	ac := i
	for i := 0; i < 4; i++ {
		ac <<= 4
		ch := b[i+offset]
		l := byte(0)
		if ch >= '0' && ch <= '9' {
			l = ch - '0'
		} else if ch >= 'a' && ch <= 'f' {
			l = ch - 'a' + 10
		} else if ch >= 'A' && ch <= 'F' {
			l = ch - 'A' + 10
		} else {
			return 0, errors.New("invalid character was found")
		}
		ac |= uint16(l)
	}
	return rune(ac), nil
}

func getStringLexeme(b []byte, offset *int, lex *lexeme) error {
	var runebuffer [4]byte
	*offset++
	lenb := len(b)
	length, err := calcStringSize(b, *offset)
	if err != nil {
		return err
	}
	arr := make([]byte, length)
	i := 0
	for ; *offset < lenb; *offset++ {
		ch := b[*offset]
		if ch == '"' {
			*offset++
			break
		}
		if ch != '\\' {
			arr[i] = ch
			i++
			continue
		}
		*offset++
		ch = b[*offset]
		switch ch {
		case 'r':
			arr[i] = 13
		case 'n':
			arr[i] = 10
		case 'b':
			arr[i] = 8
		case 't':
			arr[i] = 9
		case 'f':
			arr[i] = 12
		case '"', '\\', '/':
			arr[i] = ch
		case 'u':
			*offset++
			run, err := getRune(b, *offset)
			if err != nil {
				return err
			}
			cnt := utf8.EncodeRune(runebuffer[:], run)
			copy(arr[i:], runebuffer[0:cnt])
			i += cnt
		default:
			return errors.New("invalid token was found")
		}
	}
	lex.str = string(arr)
	lex.lexType = ltString
	return nil
}

func getNumberLexeme(b []byte, offset *int, lex *lexeme) error {
	lenb := len(b)
	intPart := 0
	isNegative := false
	start := *offset
	if b[*offset] == '-' {
		isNegative = true
		*offset++
		if *offset == lenb {
			return errors.New("invalid token was found")
		}
		if b[*offset] == '.' {
			return errors.New("invalid token was found")
		}
	}
	fractalFound := false

	// calc int part
	for ; *offset < lenb; *offset++ {
		ch := b[*offset]
		if ch >= '0' && ch <= '9' {
			intPartTmp := intPart*10 + int(ch-'0')
			if intPartTmp < intPart {
				return errors.New("overload int part of the digit")
			}
			intPart = intPartTmp
			continue
		} else if ch == '.' {
			fractalFound = true
			break
		} else if ch == '}' || ch == ']' || ch == ',' || ch == '/' || ch == '\t' || ch == '\r' || ch == '\n' || ch == ' ' {
			break
		}
		return errors.New("invalid token was found")
	}
	if isNegative {
		intPart = -intPart
	}
	if !fractalFound {
		lex.intValue = intPart
		lex.lexType = ltInteger
		return nil
	}
	*offset++
	for ; *offset < lenb; *offset++ {
		ch := b[*offset]
		if (ch >= '0' && ch <= '9') || ch == 'e' || ch == 'E' || ch == '+' || ch == '-' {
			continue
		}
		if ch == '}' || ch == ']' || ch == ',' || ch == '\t' || ch == '\r' || ch == '\n' || ch == ' ' || ch == '/' {
			break
		}
		return errors.New("invalid token was found in fractal part")
	}
	float, err := strconv.ParseFloat(string(b[start:*offset]), 64)
	if err != nil {
		return err
	}
	lex.realValue = float
	lex.lexType = ltReal
	return nil
}

func skipEmpty(b []byte, offset *int) error {
	lenb := len(b)
mainloop:
	for ; *offset < lenb; *offset++ {
		ch := b[*offset]
		if ch == '\t' || ch == '\r' || ch == '\n' || ch == ' ' {
			continue
		}
		// comments check. JSON not supported json but I appended such functionality
		if ch == '/' {
			if *offset == lenb-1 {
				return errors.New("invalid lexeme was not found")
			}
			*offset++
			ch := b[*offset]
			if ch == '/' {
				*offset++
				for ; *offset < lenb; *offset++ {
					ch := b[*offset]
					if ch == '\r' || ch == '\n' {
						continue mainloop
					}
				}
				continue
			} else if ch == '*' {
				*offset++
				for ; *offset < lenb-1; *offset++ {
					ch := b[*offset]
					if ch == '*' || b[*offset+1] == '/' {
						*offset++
						continue mainloop
					}
				}

			} else {
				return errors.New("invalid lexeme was found")
			}
		}
		// break
		return nil
	}
	return eof{}
}

func getLexeme(b []byte, offset *int, lex *lexeme) error {
	var buf [5]byte
	err := skipEmpty(b, offset)
	if err != nil {
		return err
	}
	lenb := len(b)
	ch := b[*offset]
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
			if ch == '}' || ch == ']' || ch == ',' || ch == '/' {
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
		obj, err := parseObj(b, offset, lex)
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
			return nil, errors.New("string lexeme was not found")
		}
		key := lex.str
		err = getLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltSmCommand || lex.intValue != smColon {
			return nil, errors.New("colon lexeme was not found")
		}
		obj, err := parseObj(b, offset, lex)
		if err != nil {
			return nil, err
		}
		tmp.Add(key, obj)
		err = getLexeme(b, offset, lex)
		if err != nil {
			return nil, err
		}
		if lex.lexType != ltSmCommand {
			return nil, errors.New("string lexeme was not found")
		}
		if lex.intValue == smCloseDictionary {
			return tmp, nil
		}
		if lex.intValue != smComma {
			return nil, errors.New("invalid lexeme was found")
		}
	}
}

func parseObj(b []byte, offset *int, lex *lexeme) (obj CustomDataType, er error) {
	err := getLexeme(b, offset, lex)
	if err != nil {
		return nil, err
	}
	switch lex.lexType {
	case ltNull:
		return CreateNull(), nil
	case ltInteger:
		return CreateInt(lex.intValue), nil
	case ltReal:
		return CreateReal(lex.realValue), nil
	case ltBoolean:
		return CreateBool(lex.intValue != 0), nil
	case ltString:
		return CreateString(lex.str), nil
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
	return nil, errors.New("unknow lexeme was found")
}

func LoadJSONObj(b []byte) (obj CustomDataType, er error) {
	offset := 0
	obj, err := LoadOneJSONObj(b, &offset)
	if err != nil {
		return nil, err
	}
	finished := offset
	lex := &lexeme{}
	_, err = parseObj(b, &offset, lex)
	_, iseof := err.(eof)
	if !iseof {
		return nil, errors.New("extra data present from " + strconv.Itoa(finished))
	}
	return obj, nil
}

func LoadOneJSONObj(b []byte, offset *int) (obj CustomDataType, er error) {
	lex := new(lexeme)
	if *offset < 0 {
		*offset = 0
	}
	/*	defer func() {
		if er != nil {
			fmt.Printf("'Error %s in %s\roffset:%d\r%s\r", er.Error(), string(b), offset, string(b[*offset:]))
		}
	}()*/
	return parseObj(b, offset, lex)
}
