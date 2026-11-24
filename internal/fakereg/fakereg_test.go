package fakereg

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestThrottle(t *testing.T) {
	reg := New(t)
	reg.Throttle = 200_000
	blob := reg.AddImage("team/app", "v1", 50_000).Layers[0]
	url := "http://" + reg.Host + "/v2/team/app/blobs/" + blob.Digest
	size := len(blob.Data)
	// The last chunk, Throttle/20 bytes, is sent without a wait after it.
	least := func(n int) time.Duration {
		return time.Duration(float64(n-reg.Throttle/20) / float64(reg.Throttle) * float64(time.Second))
	}

	start := time.Now()
	body, err := get(url, "")
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < least(size) {
		t.Errorf("the blob took %v, want at least %v", elapsed, least(size))
	}
	if !bytes.Equal(body, blob.Data) {
		t.Fatal("the throttled body is not the blob")
	}

	body, err = get(url, "bytes=1000-")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, blob.Data[1000:]) {
		t.Fatal("the throttled range is not the blob's")
	}

	blob.Faults(Cut)
	start = time.Now()
	body, err = get(url, "")
	if err == nil {
		t.Fatal("a cut body read to its end")
	}
	if len(body) != size/2 {
		t.Fatalf("the cut body has %d bytes, want %d", len(body), size/2)
	}
	if elapsed := time.Since(start); elapsed < least(size/2) {
		t.Errorf("the cut body took %v, want at least %v", elapsed, least(size/2))
	}

	log := waitLog(t, reg, 3)
	for i, want := range []int64{int64(size), int64(size - 1000), int64(size / 2)} {
		if log[i].Bytes != want {
			t.Errorf("request %d logged %d bytes, want %d", i, log[i].Bytes, want)
		}
	}
}

func TestUnthrottledBytes(t *testing.T) {
	reg := New(t)
	blob := reg.AddImage("team/app", "v1", 5000).Layers[0]
	url := "http://" + reg.Host + "/v2/team/app/blobs/" + blob.Digest
	body, err := get(url, "")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, blob.Data) {
		t.Fatal("the body is not the blob")
	}
	if got := waitLog(t, reg, 1)[0].Bytes; got != int64(len(blob.Data)) {
		t.Fatalf("logged %d bytes, want %d", got, len(blob.Data))
	}
}

// get GETs url with an optional Range and returns the body it read, with the
// read's error.
func get(url, rng string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	return io.ReadAll(resp.Body)
}

// waitLog waits until the registry logged n requests: a handler logs its
// request after the client may have read the whole body.
func waitLog(t *testing.T, reg *Registry, n int) []Request {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		log := reg.Log()
		if len(log) >= n {
			return log
		}
		if time.Now().After(deadline) {
			t.Fatalf("the registry logged %d requests, want %d", len(log), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
