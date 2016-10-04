package worker

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

var wg sync.WaitGroup

func ParseLineToJSON(in <-chan []byte) (out chan map[string]interface{}) {
	out = make(chan map[string]interface{}, 2000)
	n := 4
	done := make(chan bool, n-1)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range in {
				j := make(map[string]interface{})
				err := json.Unmarshal(line, &j)
				if err != nil {
					panic(err)
				}
				out <- j
			}
			select {
			case done <- true:
			default:
				close(out)
			}
		}()
	}
	return
}

func JSONExtractText(in <-chan map[string]interface{}) (out chan string) {
	out = make(chan string, 4000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := range in {
			text := j["text"].(string)
			out <- text
		}
		close(out)
	}()
	return
}

func TokenizeText(in <-chan string) (out chan string) {
	out = make(chan string, 40000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for text := range in {
			scanner := bufio.NewScanner(strings.NewReader(text))
			scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
				start := 0
				for width := 0; start < len(data); start += width {
					var r rune
					r, width = utf8.DecodeRune(data[start:])
					if unicode.IsLetter(r) {
						break
					}
				}
				for width, i := 0, start; i < len(data); i += width {
					var r rune
					r, width = utf8.DecodeRune(data[i:])
					if !unicode.IsLetter(r) {
						if r == '-' {
							if r, _ = utf8.DecodeRune(data[i + width:]); unicode.IsLetter(r) {
								continue
							}
						}
						return i + width, data[start:i], nil
					}
				}
				if atEOF && len(data) > start {
					return len(data), data[start:], nil
				}
				return start, nil, nil
			})
			for scanner.Scan() {
				text := scanner.Text()
				out <- text
			}
			if err := scanner.Err(); err != nil {
				panic(err)
			}
		}
		close(out)
	}()
	return
}

func DumpJSON(in <-chan map[string]interface{}) {
	wg.Add(1)
	defer wg.Done()
	var j interface{}
	for j = range in {
		if title, exists := j.(map[string]interface{})["title"]; exists {
			text := j.(map[string]interface{})["text"]
			fmt.Printf(">>%s\n%s\n", title, text)
		}
	}
}

func DumpString(in <-chan string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var s string
		for s = range in {
			fmt.Println(s)
		}
	}()
}

func ReadFile(file_name string) (out chan []byte) {
	out = make(chan []byte, 2000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		file, err := os.Open(file_name)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		gzfile, err := gzip.NewReader(file)
		if err != nil {
			panic(err)
		}
		defer gzfile.Close()

		buf := bufio.NewReader(gzfile)
	Loop:
		for {
			var line []byte
			for i := 0; i < 2; i++ {
				line, err = buf.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						close(out)
						break Loop
					}
					panic(err)
				}
			}
			out <- line
		}
	}()
	return
}

func Wait() {
	wg.Wait()
}
