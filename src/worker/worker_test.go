package worker

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestFileServer(t *testing.T) {
	tests := []struct {
		Name    string
		content string
		file    string
		target  string
	}{
		{"correct", "hello world", "test.txt", "test.txt"},
		{"incorrect", "hello world", "test.txt", "foo.txt"},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			dir := t.TempDir()
			wd := MakeWorkerDaemon("8.8.8.8:80", WithBaseDir(dir))
			wd.startFileServer()
			wd.setAddr()
			path := filepath.Join(dir, tt.file)
			file, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			file.Write([]byte(tt.content))
			file.Close()
			addr := fmt.Sprintf("http://%v/%v", wd.addr(), tt.target)
			resp, err := http.Get(addr)
			if err != nil {
				t.Fatalf("GET %v: %v", addr, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 && tt.file == tt.target {
				t.Fatalf("GET %v: expected status code 200, got %d", addr, resp.StatusCode)
			}
			bytes, err := io.ReadAll(resp.Body)
			if string(bytes) != tt.content && tt.file == tt.target {
				t.Fatalf("GET %v: expected 'test', got '%v'", addr, string(bytes))
			}
		})
	}

}
