package datamodel

import (
	//"fmt"
	"testing"
	"time"
)

func typeObject(t *testing.T, obj CustomDataType) string {
	l := obj.getLength()
	m := make([]byte, l)
	_, err := obj.writeToBytes(m)
	if err != nil {
		t.Fatalf("Calculation returns error: %s", err.Error())
	}
	return string(m)
}

func getType(obj CustomDataType) string {
	switch obj.(type) {
	case DataDictionary:
		return "Dictionary"
	case DataNull:
		return "Null"
	case DataBool:
		return "Bool"
	case DataInt:
		return "Int"
	case DataReal:
		return "Real"
	case DataString:
		return "String"
	case DataArray:
		return "Array"
	}
	return "Base"
}

func checkResult(t *testing.T, obj CustomDataType, waitingResult string) {
	s := typeObject(t, obj)
	if s != waitingResult {
		t.Fatalf("\nobj:    %s\nvalue:  %s\nwait:   %s", getType(obj), s, waitingResult)
	}
}

func TestArrayWork(t *testing.T) {
	arr := CreateArray(10)
	checkResult(t, arr, "[]")
	arr.Add(CreateNull())
	checkResult(t, arr, "[null]")
	arr.Add(CreateNull())
	checkResult(t, arr, "[null, null]")
	arr.Remove(0)
	checkResult(t, arr, "[null]")
	arr.Add(CreateBool(true))
	checkResult(t, arr.Get(100), "null")
	checkResult(t, arr.Get(1), "true")
	arr.Add(CreateBool(false))
	checkResult(t, arr, "[null, true, false]")
	arr.Remove(1)
	checkResult(t, arr, "[null, false]")
	for i := 0; i < 100; i++ {
		arr.Add(CreateBool(true))
	}
	for i := 0; i < 100; i++ {
		arr.Remove(0)
	}
	arr.Add(CreateBool(true), CreateBool(false))
	checkResult(t, arr, "[true, true, true, false]")
	arr.Insert(1, CreateBool(false))
	checkResult(t, arr, "[true, false, true, true, false]")
	idx := arr.Count()
	arr.Remove(idx)
	if idx != arr.Count() {
		t.Fatal("Must not change count of the elements")
	}

}

func TestDictionaryWork(t *testing.T) {
	dict := CreateDictionary(10)
	dict.Add("Test", CreateArray(10))
	checkResult(t, dict, "{\"Test\":[]}")
	dict.Add("Test1", CreateBool(true))
	dict.Add("Test", CreateNull())
	checkResult(t, dict, "{\"Test1\":true}")
	dict.Add("Test", CreateArray(0))
	z := dict.Count()
	if z != 2 {
		t.Fatalf("Dictionary element count %d, waited 2", z)
	}
	a := dict.Value("Test1")
	checkResult(t, a, "true")
	a.(DataBool).Set(false)

	a = dict.Value("Test")
	checkResult(t, a, "[]")

	a = dict.Value("Unknown")
	checkResult(t, a, "null")

	dict.Add("Test", CreateNull())
	checkResult(t, dict, "{\"Test1\":false}")
	arr := CreateArray(0)
	d1 := CreateDictionary(10)
	arr.Add(CreateDictionary(10), d1)
	dict.Add("Test1", arr)
	arr.Add(
		CreateNull(),
		CreateBool(false),
		CreateBool(true),
		CreateInt(100),
		CreateInt(-100),
		CreateReal(3.14),
		CreateString("3.14000 \t \r \n \" \\ \b  \f / test Русский язык "),
	)
	dict.Add("Test2", CreateInt(100))
	/*var test CustomDataType
	test = dict
	l := test.getLength()
	for i := 0; i < l; i++ {
		m := make([]byte, i)
		if _, err := test.writeToBytes(m); err == nil {
			t.Fatalf("Function must return Error, %d %s", i, string(m))
		}
	}*/

}

func TestPrimitives(t *testing.T) {
	checkResult(t, CreateNull(), "null")
	checkResult(t, CreateBool(false), "false")
	checkResult(t, CreateInt(-100), "-100")
	checkResult(t, CreateReal(3.14), "3.14")
	checkResult(t, CreateString("\000 3.1400 \t \r \n \" \\ test Русский язык "), "\"\\u0000 3.1400 \\t \\r \\n \\\" \\\\ test Русский язык \"")
	a := CreateBool(false)
	a.Set(!a.Get())
	checkResult(t, a, "true")
	b := CreateInt(100)
	b.Set(b.Get() + 1)
	checkResult(t, b, "101")
	c := CreateReal(3.14)
	c.Set(-c.Get())
	checkResult(t, c, "-3.14")
	d := CreateString("Test")
	d.Set(d.Get() + " passed")
	checkResult(t, d, "\"Test passed\"")
}

var mustFailLexeme = []string{
	"-",
	"-1.10s",
	"-.10",
	"-1.10\\",
	"1111111111111111111111111111111111111111111",
	"1k",
	"1.01e-1d",
	`"test\u00"`,
	`"test\u003"`,
	"",
	"/",
	"//",
	"invalid",
	`inva\tlid`,
	"/\rnull",
	"/*\rnull",
	" \r/**/",
	"\"\testsrtdsasdsasdsadas\\\\ \\r \\u10Fa",
	`"test`,
	`"test\u005 "`,
	`"test\ "`,
	`"test\s "`,
	`"test\u"`,
	`"test\u0"`,
	`"test \"`,
	"abc",
	`"\`,
	`"\z"`,
}

var mustPassLexeme = []string{
	"-1.10]",
	"1.10",
	"1234 ",
	"4455.5e-1",
	"// some error \r null",
	"\"\\testsrtdsasdsasdsadas\\\\ \\r \\u10Fa \"",
	`"\u005c \t a \r alses \f \b \n"
	`,
	`" rt" /* testcomment */`,
	"\rnull",
	"//test\rnull",
	"//test\rnull",
	"/**/null",
	"{}",
	"[]",
	//	"\"testsrt\"",

}

func TestParsingLexeme(t *testing.T) {
	for i := 0; i < len(mustFailLexeme); i++ {
		//		fmt.Printf("Step %d\n%s\n", i, mustFailLexeme[i])
		data := []byte(mustFailLexeme[i])
		off := 0
		lex := new(lexeme)
		err := getLexeme(data, &off, lex)
		if err == nil {
			t.Fatalf("Step %d\r Bellow lexeme must be parsed with error\r%s", i, mustFailLexeme[i])
		}
	}
	for i := 0; i < len(mustPassLexeme); i++ {
		//		fmt.Printf("Step %d\n%s\n", i, mustPassLexeme[i])
		data := []byte(mustPassLexeme[i])
		off := 0
		lex := new(lexeme)
		err := getLexeme(data, &off, lex)
		if err != nil {
			t.Fatalf("Step %d\r Bellow lexeme must be parsed without error\r%s\r Bellow error was received \r%s", i, mustPassLexeme[i], err.Error())
		}
	}
}

var mustFailObjects = []string{
	"{1:1}",
	"{1 :1}",
	"[abc",
	"[}",
	"[null null]",
	"[null [",
	"[null error",

	`{"1":}`,
	`{"11"{}}`,
	`{"11":100 1}`,
	`{"1:":error}`,
	`{"1";1}`,
	`{"1":100]`,
	"someerror",
}

var mustPassObjects = []string{
	`{"a1":[true,null," rt" /* testcomment */],
			"b2":false//,
			,
			//
			"c3":"testOther"}`,
	"{}",
	"[]",
}

func TestParsingObjects(t *testing.T) {
	for i := 0; i < len(mustFailObjects); i++ {
		//		fmt.Printf("Step %d\n%s\n", i, mustFailLexeme[i])
		data := []byte(mustFailObjects[i])
		off := 0
		_, err := LoadOneJSONObj(data, &off)
		if err == nil {
			t.Fatalf("Step %d\r Bellow lexeme must be parsed with error\r%s", i, mustFailLexeme[i])
		}
	}
	for i := 0; i < len(mustPassObjects); i++ {
		//		fmt.Printf("Step %d\n%s\n", i, mustPassLexeme[i])
		data := []byte(mustPassObjects[i])
		off := -1
		_, err := LoadOneJSONObj(data, &off)
		if err != nil {
			t.Fatalf("Step %d\r Bellow lexeme must be parsed without error\r%s\r Bellow error was received \r%s", i, mustPassLexeme[i], err.Error())
		}
	}

}

func TestParsing(t *testing.T) {

	offset := 0
	lex := new(lexeme)
	obj, err := parseObj([]byte("[true, false, null]"), &offset, lex)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	checkResult(t, obj, "[true, false, null]")

	strongTest := `
		{"a1":[true,null," rt" /* testcomment */],
			"b2":false//,
			,
			//
			"c3":"testOther"}`

	offset = 0
	_, err = parseObj([]byte(strongTest), &offset, lex)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	strongTest = `
		{"ccc1":[true,null," rt" /* testcomment */, 100.4 ,-5, 90, 4.01e+5, 0]
			}`

	offset = 0
	_, err = parseObj([]byte(strongTest), &offset, lex)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	_, err = LoadJSONObj([]byte(strongTest))
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	strongTest = strongTest + `/*  */ [true]`
	_, err = LoadJSONObj([]byte(strongTest))
	if err == nil {
		t.Fatal("Error object must be found")
	}
	_, err = LoadJSONObj([]byte(longjson))
	if err != nil {
		t.Fatal("Error object must be found")
	}

}

var longjson = `
{
  "statuses": [
    {
      "coordinates": null,
      "favorited": false,
      "truncated": false,
      "created_at": "Mon Sep 24 03:35:21 +0000 2012",
      "id_str": "250075927172759552",
      "entities": {
        "urls": [
 
        ],
        "hashtags": [
          {
            "text": "freebandnames",
            "indices": [
              20,
              34
            ]
          }
        ],
        "user_mentions": [
 
        ]
      },
      "in_reply_to_user_id_str": null,
      "contributors": null,
      "text": "Aggressive Ponytail #freebandnames",
      "metadata": {
        "iso_language_code": "en",
        "result_type": "recent"
      },
      "retweet_count": 0,
      "in_reply_to_status_id_str": null,
      "id": 250075927172759552,
      "geo": null,
      "retweeted": false,
      "in_reply_to_user_id": null,
      "place": null,
      "user": {
        "profile_sidebar_fill_color": "DDEEF6",
        "profile_sidebar_border_color": "C0DEED",
        "profile_background_tile": false,
        "name": "Sean Cummings",
        "profile_image_url": "http://a0.twimg.com/profile_images/2359746665/1v6zfgqo8g0d3mk7ii5s_normal.jpeg",
        "created_at": "Mon Apr 26 06:01:55 +0000 2010",
        "location": "LA, CA",
        "follow_request_sent": null,
        "profile_link_color": "0084B4",
        "is_translator": false,
        "id_str": "137238150",
        "entities": {
          "url": {
            "urls": [
              {
                "expanded_url": null,
                "url": "",
                "indices": [
                  0,
                  0
                ]
              }
            ]
          },
          "description": {
            "urls": [
 
            ]
          }
        },
        "default_profile": true,
        "contributors_enabled": false,
        "favourites_count": 0,
        "url": null,
        "profile_image_url_https": "https://si0.twimg.com/profile_images/2359746665/1v6zfgqo8g0d3mk7ii5s_normal.jpeg",
        "utc_offset": -28800,
        "id": 137238150,
        "profile_use_background_image": true,
        "listed_count": 2,
        "profile_text_color": "333333",
        "lang": "en",
        "followers_count": 70,
        "protected": false,
        "notifications": null,
        "profile_background_image_url_https": "https://si0.twimg.com/images/themes/theme1/bg.png",
        "profile_background_color": "C0DEED",
        "verified": false,
        "geo_enabled": true,
        "time_zone": "Pacific Time (US & Canada)",
        "description": "Born 330 Live 310",
        "default_profile_image": false,
        "profile_background_image_url": "http://a0.twimg.com/images/themes/theme1/bg.png",
        "statuses_count": 579,
        "friends_count": 110,
        "following": null,
        "show_all_inline_media": false,
        "screen_name": "sean_cummings"
      },
      "in_reply_to_screen_name": null,
      "source": "<a href=\"//itunes.apple.com/us/app/twitter/id409789998?mt=12%5C%22\" rel=\"\\\"nofollow\\\"\">Twitter for Mac</a>",
      "in_reply_to_status_id": null
    },
    {
      "coordinates": null,
      "favorited": false,
      "truncated": false,
      "created_at": "Fri Sep 21 23:40:54 +0000 2012",
      "id_str": "249292149810667520",
      "entities": {
        "urls": [
 
        ],
        "hashtags": [
          {
            "text": "FreeBandNames",
            "indices": [
              20,
              34
            ]
          }
        ],
        "user_mentions": [
 
        ]
      },
      "in_reply_to_user_id_str": null,
      "contributors": null,
      "text": "Thee Namaste Nerdz. #FreeBandNames",
      "metadata": {
        "iso_language_code": "pl",
        "result_type": "recent"
      },
      "retweet_count": 0,
      "in_reply_to_status_id_str": null,
      "id": 249292149810667520,
      "geo": null,
      "retweeted": false,
      "in_reply_to_user_id": null,
      "place": null,
      "user": {
        "profile_sidebar_fill_color": "DDFFCC",
        "profile_sidebar_border_color": "BDDCAD",
        "profile_background_tile": true,
        "name": "Chaz Martenstein",
        "profile_image_url": "http://a0.twimg.com/profile_images/447958234/Lichtenstein_normal.jpg",
        "created_at": "Tue Apr 07 19:05:07 +0000 2009",
        "location": "Durham, NC",
        "follow_request_sent": null,
        "profile_link_color": "0084B4",
        "is_translator": false,
        "id_str": "29516238",
        "entities": {
          "url": {
            "urls": [
              {
                "expanded_url": null,
                "url": "http://bullcityrecords.com/wnng/",
                "indices": [
                  0,
                  32
                ]
              }
            ]
          },
          "description": {
            "urls": [
 
            ]
          }
        },
        "default_profile": false,
        "contributors_enabled": false,
        "favourites_count": 8,
        "url": "http://bullcityrecords.com/wnng/",
        "profile_image_url_https": "https://si0.twimg.com/profile_images/447958234/Lichtenstein_normal.jpg",
        "utc_offset": -18000,
        "id": 29516238,
        "profile_use_background_image": true,
        "listed_count": 118,
        "profile_text_color": "333333",
        "lang": "en",
        "followers_count": 2052,
        "protected": false,
        "notifications": null,
        "profile_background_image_url_https": "https://si0.twimg.com/profile_background_images/9423277/background_tile.bmp",
        "profile_background_color": "9AE4E8",
        "verified": false,
        "geo_enabled": false,
        "time_zone": "Eastern Time (US & Canada)",
        "description": "You will come to Durham, North Carolina. I will sell you some records then, here in Durham, North Carolina. Fun will happen.",
        "default_profile_image": false,
        "profile_background_image_url": "http://a0.twimg.com/profile_background_images/9423277/background_tile.bmp",
        "statuses_count": 7579,
        "friends_count": 348,
        "following": null,
        "show_all_inline_media": true,
        "screen_name": "bullcityrecords"
      },
      "in_reply_to_screen_name": null,
      "source": "web",
      "in_reply_to_status_id": null
    },
    {
      "coordinates": null,
      "favorited": false,
      "truncated": false,
      "created_at": "Fri Sep 21 23:30:20 +0000 2012",
      "id_str": "249289491129438208",
      "entities": {
        "urls": [
 
        ],
        "hashtags": [
          {
            "text": "freebandnames",
            "indices": [
              29,
              43
            ]
          }
        ],
        "user_mentions": [
 
        ]
      },
      "in_reply_to_user_id_str": null,
      "contributors": null,
      "text": "Mexican Heaven, Mexican Hell #freebandnames",
      "metadata": {
        "iso_language_code": "en",
        "result_type": "recent"
      },
      "retweet_count": 0,
      "in_reply_to_status_id_str": null,
      "id": 249289491129438208,
      "geo": null,
      "retweeted": false,
      "in_reply_to_user_id": null,
      "place": null,
      "user": {
        "profile_sidebar_fill_color": "99CC33",
        "profile_sidebar_border_color": "829D5E",
        "profile_background_tile": false,
        "name": "Thomas John Wakeman",
        "profile_image_url": "http://a0.twimg.com/profile_images/2219333930/Froggystyle_normal.png",
        "created_at": "Tue Sep 01 21:21:35 +0000 2009",
        "location": "Kingston New York",
        "follow_request_sent": null,
        "profile_link_color": "D02B55",
        "is_translator": false,
        "id_str": "70789458",
        "entities": {
          "url": {
            "urls": [
              {
                "expanded_url": null,
                "url": "",
                "indices": [
                  0,
                  0
                ]
              }
            ]
          },
          "description": {
            "urls": [
 
            ]
          }
        },
        "default_profile": false,
        "contributors_enabled": false,
        "favourites_count": 19,
        "url": null,
        "profile_image_url_https": "https://si0.twimg.com/profile_images/2219333930/Froggystyle_normal.png",
        "utc_offset": -18000,
        "id": 70789458,
        "profile_use_background_image": true,
        "listed_count": 1,
        "profile_text_color": "3E4415",
        "lang": "en",
        "followers_count": 63,
        "protected": false,
        "notifications": null,
        "profile_background_image_url_https": "https://si0.twimg.com/images/themes/theme5/bg.gif",
        "profile_background_color": "352726",
        "verified": false,
        "geo_enabled": false,
        "time_zone": "Eastern Time (US & Canada)",
        "description": "Science Fiction Writer, sort of. Likes Superheroes, Mole People, Alt. Timelines.",
        "default_profile_image": false,
        "profile_background_image_url": "http://a0.twimg.com/images/themes/theme5/bg.gif",
        "statuses_count": 1048,
        "friends_count": 63,
        "following": null,
        "show_all_inline_media": false,
        "screen_name": "MonkiesFist"
      },
      "in_reply_to_screen_name": null,
      "source": "web",
      "in_reply_to_status_id": null
    },
    {
      "coordinates": null,
      "favorited": false,
      "truncated": false,
      "created_at": "Fri Sep 21 22:51:18 +0000 2012",
      "id_str": "249279667666817024",
      "entities": {
        "urls": [
 
        ],
        "hashtags": [
          {
            "text": "freebandnames",
            "indices": [
              20,
              34
            ]
          }
        ],
        "user_mentions": [
 
        ]
      },
      "in_reply_to_user_id_str": null,
      "contributors": null,
      "text": "The Foolish Mortals #freebandnames",
      "metadata": {
        "iso_language_code": "en",
        "result_type": "recent"
      },
      "retweet_count": 0,
      "in_reply_to_status_id_str": null,
      "id": 249279667666817024,
      "geo": null,
      "retweeted": false,
      "in_reply_to_user_id": null,
      "place": null,
      "user": {
        "profile_sidebar_fill_color": "BFAC83",
        "profile_sidebar_border_color": "615A44",
        "profile_background_tile": true,
        "name": "Marty Elmer",
        "profile_image_url": "http://a0.twimg.com/profile_images/1629790393/shrinker_2000_trans_normal.png",
        "created_at": "Mon May 04 00:05:00 +0000 2009",
        "location": "Wisconsin, USA",
        "follow_request_sent": null,
        "profile_link_color": "3B2A26",
        "is_translator": false,
        "id_str": "37539828",
        "entities": {
          "url": {
            "urls": [
              {
                "expanded_url": null,
                "url": "http://www.omnitarian.me",
                "indices": [
                  0,
                  24
                ]
              }
            ]
          },
          "description": {
            "urls": [
 
            ]
          }
        },
        "default_profile": false,
        "contributors_enabled": false,
        "favourites_count": 647,
        "url": "http://www.omnitarian.me",
        "profile_image_url_https": "https://si0.twimg.com/profile_images/1629790393/shrinker_2000_trans_normal.png",
        "utc_offset": -21600,
        "id": 37539828,
        "profile_use_background_image": true,
        "listed_count": 52,
        "profile_text_color": "000000",
        "lang": "en",
        "followers_count": 608,
        "protected": false,
        "notifications": null,
        "profile_background_image_url_https": "https://si0.twimg.com/profile_background_images/106455659/rect6056-9.png",
        "profile_background_color": "EEE3C4",
        "verified": false,
        "geo_enabled": false,
        "time_zone": "Central Time (US & Canada)",
        "description": "Cartoonist, Illustrator, and T-Shirt connoisseur",
        "default_profile_image": false,
        "profile_background_image_url": "http://a0.twimg.com/profile_background_images/106455659/rect6056-9.png",
        "statuses_count": 3575,
        "friends_count": 249,
        "following": null,
        "show_all_inline_media": true,
        "screen_name": "Omnitarian"
      },
      "in_reply_to_screen_name": null,
      "source": "<a href=\"//twitter.com/download/iphone%5C%22\" rel=\"\\\"nofollow\\\"\">Twitter for iPhone</a>",
      "in_reply_to_status_id": null
    }
  ],
  "search_metadata": {
    "max_id": 250126199840518145,
    "since_id": 24012619984051000,
    "refresh_url": "?since_id=250126199840518145&q=%23freebandnames&result_type=mixed&include_entities=1",
    "next_results": "?max_id=249279667666817023&q=%23freebandnames&count=4&include_entities=1&result_type=mixed",
    "count": 4,
    "completed_in": 0.035,
    "since_id_str": "24012619984051000",
    "query": "%23freebandnames",
    "max_id_str": "250126199840518145"
  }
}

`

func BenchmarkParseJson(b *testing.B) {
	// run the Fib function b.N times
	t := time.Now()
	tot := []byte(longjson)
	for n := 0; n < b.N; n++ {
		_, err := LoadJSONObj(tot)
		if err != nil {
			b.Fatal("Error " + err.Error())
		}
	}
	b.Log("Total time ", b.N, "  processed bytes:", b.N*len(tot), " ", time.Since(t))
}

func TestLoadAndSave(t *testing.T) {
	tot := []byte(longjson)
	offset := 0
	obj, err := LoadOneJSONObj(tot, &offset)
	if err != nil {
		t.Fatal("Error " + err.Error())
	}
	l := obj.getLength()
	m := make([]byte, l)
	_, err = obj.writeToBytes(m)
	if err != nil {
		t.Fatal("Error " + err.Error())
	}
}

func BenchmarkSaveJsonOneAlloc(b *testing.B) {
	tot := []byte(longjson)
	t := time.Now()
	offset := 0
	obj, err := LoadOneJSONObj(tot, &offset)
	if err != nil {
		b.Fatal("Error " + err.Error())
	}
	l := obj.getLength()
	m := make([]byte, l)
	for n := 0; n < b.N; n++ {

		_, err = obj.writeToBytes(m)
		if err != nil {
			b.Fatal("Error " + err.Error())
		}
	}
	b.Log("Total processed bytes:", b.N*l, " ", time.Since(t))
}

func BenchmarkSaveJsonEachAlloc(b *testing.B) {

	tot := []byte(longjson)
	t := time.Now()
	offset := 0
	obj, err := LoadOneJSONObj(tot, &offset)
	if err != nil {
		b.Fatal("Error " + err.Error())
	}
	ll := 0
	for n := 0; n < b.N; n++ {
		l := obj.getLength()
		ll += l
		m := make([]byte, l)
		_, err = obj.writeToBytes(m)
		if err != nil {
			b.Fatal("Error " + err.Error())
		}
	}
	b.Log("Total processed bytes:", ll, " ", time.Since(t))
}

//RESP section
func TestRESP(t *testing.T) {
	if string(ConvertCommandToRASP("LLEN", CreateString("mylist"))) != "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n" {
		t.Fatal("Invalid result")
	}
}
