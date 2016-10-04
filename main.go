package main

import (
	"flag"
	"github.com/bkmeneguello/wiki/worker"
	"github.com/pkg/profile"
	"time"
	"log"
)

func main() {
	mode := flag.String("profile.mode", "", "enable profiling mode, one of [cpu, mem, block]")
	flag.Parse()
	switch *mode {
	case "cpu":
		defer profile.Start(profile.CPUProfile).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile).Stop()
	case "block":
		defer profile.Start(profile.BlockProfile).Stop()
	default:
		// do nothing
	}

	line_stream := worker.ReadFile(flag.Arg(0))
	json_stream := worker.ParseLineToJSON(line_stream)
	text_stream := worker.JSONExtractText(json_stream)
	token_stream := worker.TokenizeText(text_stream)

	ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
	ticker.Stop()
	go func() {
		len_line_stream_sum := 0
		len_json_stream_sum := 0
		len_text_stream_sum := 0
		len_token_stream_sum := 0
		count := 0
		for range ticker.C {
			len_line_stream := len(line_stream)
			len_json_stream := len(json_stream)
			len_text_stream := len(text_stream)
			len_token_stream := len(token_stream)
			log.Printf("%3d/%3d %2d/%2d %3d/%3d %4d/%4d", len_line_stream, cap(line_stream), len_json_stream, cap(json_stream), len_text_stream, cap(text_stream), len_token_stream, cap(token_stream))
			len_line_stream_sum += len_line_stream
			len_json_stream_sum += len_json_stream
			len_text_stream_sum += len_text_stream
			len_token_stream_sum += len_token_stream
			count += 1
			log.Printf("%6d %6d %6d %6d", len_line_stream_sum/count, len_json_stream_sum/count, len_text_stream_sum/count, len_token_stream_sum/count)
		}
	}()

	worker.DumpString(token_stream)

	worker.Wait()
}
