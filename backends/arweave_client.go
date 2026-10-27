package backends

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"
)

type TransactionOffset struct {
	Size   string `json:"size"`
	Offset string `json:"offset"`
}

type TransactionChunk struct {
	Chunk    string `json:"chunk"`
	DataPath string `json:"data_path"`
	TxPath   string `json:"tx_path"`
}

type Client struct {
	client *http.Client
	url    string
}

const (
	// Default HTTP timeout so a hung Arweave node cannot freeze reads indefinitely.
	defaultHTTPTimeout = 30 * time.Second
	// Upper bound for a single chunk download; a server-controlled size larger
	// than this is treated as invalid rather than trusted for allocation.
	maxChunkSize = 256 << 20
)

// NewClient creates an HTTP client for the given Arweave node. It returns an
// error instead of panicking when the optional proxy URL is malformed.
func NewClient(nodeUrl string, proxyUrl ...string) (*Client, error) {
	httpClient := &http.Client{Timeout: defaultHTTPTimeout}
	// if exist proxy url
	if len(proxyUrl) > 0 {
		pUrl := proxyUrl[0]
		parsedProxyUrl, err := url.Parse(pUrl)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy url %q: %w", pUrl, err)
		}
		tr := &http.Transport{Proxy: http.ProxyURL(parsedProxyUrl)}
		httpClient = &http.Client{Transport: tr, Timeout: defaultHTTPTimeout}
	}

	return &Client{client: httpClient, url: nodeUrl}, nil
}

func (c *Client) getTransactionOffset(id string) (*TransactionOffset, error) {
	_path := fmt.Sprintf("tx/%s/offset", id)
	body, statusCode, err := c.httpGet(_path)
	// Check the transport error first: when the request itself failed,
	// statusCode is 0 and the real cause must not be masked by a
	// misleading "not found" error.
	if err != nil {
		return nil, err
	}
	if statusCode != 200 {
		return nil, fmt.Errorf("could not get tx offset for %q: HTTP %d", id, statusCode)
	}
	txOffset := &TransactionOffset{}
	if err := json.Unmarshal(body, txOffset); err != nil {
		return nil, err
	}
	return txOffset, nil
}

func (c *Client) httpGet(_path string) (body []byte, statusCode int, err error) {
	u, err := url.Parse(c.url)
	if err != nil {
		return
	}

	u.Path = path.Join(u.Path, _path)

	resp, err := c.client.Get(u.String())
	if err != nil {
		return
	}
	defer resp.Body.Close()

	statusCode = resp.StatusCode
	body, err = io.ReadAll(resp.Body)
	return
}

func (c *Client) DownloadChunkData(id string) ([]byte, error) {
	offsetResponse, err := c.getTransactionOffset(id)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseInt(offsetResponse.Size, 10, 64)
	if err != nil {
		return nil, err
	}
	endOffset, err := strconv.ParseInt(offsetResponse.Offset, 10, 64)
	if err != nil {
		return nil, err
	}
	// The size and offsets are server-controlled strings; sanity-check them so
	// a malicious/misreported node cannot trigger huge allocations, negative
	// offsets, or an infinite loop.
	if size <= 0 || size > maxChunkSize {
		return nil, fmt.Errorf("invalid chunk size for tx %q: %d", id, size)
	}
	startOffset := endOffset - size + 1
	if startOffset < 0 || endOffset < 0 {
		return nil, fmt.Errorf("invalid chunk offsets for tx %q: start=%d end=%d", id, startOffset, endOffset)
	}
	data := make([]byte, 0, size)
	for i := int64(0); i+startOffset < endOffset; {
		chunkData, err := c.getChunkData(i + startOffset)
		if err != nil {
			return nil, err
		}
		// Guard against a broken node returning empty chunks, which would
		// otherwise make this loop spin forever holding an open HTTP reader.
		if len(chunkData) == 0 {
			return nil, fmt.Errorf("received empty chunk at offset %d for tx %q", i+startOffset, id)
		}
		data = append(data, chunkData...)
		i += int64(len(chunkData))
	}
	return data, nil
}

func (c *Client) getChunkData(offset int64) ([]byte, error) {
	chunk, err := c.getChunk(offset)
	if err != nil {
		return nil, err
	}
	return base64.RawURLEncoding.DecodeString(chunk.Chunk)
}

func (c *Client) getChunk(offset int64) (*TransactionChunk, error) {
	_path := "chunk/" + strconv.FormatInt(offset, 10)
	body, statusCode, err := c.httpGet(_path)
	// Check the transport error before the status code for the same reason
	// as getTransactionOffset above.
	if err != nil {
		return nil, err
	}
	if statusCode != 200 {
		return nil, fmt.Errorf("could not get chunk data at offset %d: HTTP %d", offset, statusCode)
	}
	txChunk := &TransactionChunk{}
	if err := json.Unmarshal(body, txChunk); err != nil {
		return nil, err
	}
	return txChunk, nil
}
