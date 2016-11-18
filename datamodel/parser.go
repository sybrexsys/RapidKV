package datamodel

const (
	ltInteger   = iota /* Integer Lexeme       */
	ltReal             /* Real Lexeme          */
	ltEOF              /* End of File Lexeme   */
	ltBoolean          /* Boolean Lexeme       */
	ltNull             /* NULL Lexeme          */
	ltSmCommand        /* Small Command Lexeme */
	ltName             /* Name Lexeme          */
	ltCommand          /* Command Lexeme       */
	ltString           /* String Lexeme        */
	ltStringHex        /* Hex String Lexeme    */
)

const (
	smOpenArray       = iota /* Small Command Lexeme Open Array      */
	smCloseArray             /* Small Command Lexeme Close Array     */
	smOpenDictionary         /* Small Command Lexeme Open Dictionary */
	smCloseDictionary        /* Small Command Lexeme CloseDictionary */
)

type lexeme struct {
	lexType   int     /* Lexemå type                              */
	str       string  /* Pointer where stored data after parse    */
	intValue  int     /* Return value for Integer lexemes         */
	realValue float64 /* Return value for real lexemes            */
}

type parser struct {
}

/*
func getLexema(p parser) (*lexeme, error) {
    return nil
}

func ParseObj(p parser ) (DataBase, error) {
    lex, err := getLexema(p)
    if err != nil {
        return nil, err
    }
    switch lex.lexType {
    case ltInteger:
    case ltBoolean:
    case ltNull: return dataNull, nil
    case ltString:
    case ltEOF:
    case ltSmCommand:

    }
	return
}
*/
