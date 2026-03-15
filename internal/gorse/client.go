package gorse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

type RecommendResult struct {
	UserID string   `json:"userId"`
	Items  []string `json:"items"`
}

type feedbackRecord struct {
	FeedbackType string `json:"FeedbackType"`
	UserId       string `json:"UserId"`
	ItemId       string `json:"ItemId"`
	Timestamp    string `json:"Timestamp,omitempty"`
}

func New(baseURL, apiKey string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  strings.TrimSpace(apiKey),
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *Client) Healthy(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodGet, "/api/health/live", nil)
	if err == nil {
		return nil
	}
	_, err2 := c.do(ctx, http.MethodGet, "/api/health", nil)
	if err2 == nil {
		return nil
	}
	if err2 != nil {
		return err2
	}
	return err
}

func (c *Client) UpsertUser(ctx context.Context, userID string, labels []string) error {
	payload := map[string]any{
		"UserId": userID,
		"Labels": labels,
	}
	_, err := c.do(ctx, http.MethodPost, "/api/user", payload)
	return err
}

func (c *Client) UpsertItem(ctx context.Context, itemID string, categories []string, labels []string, comment string) error {
	payload := map[string]any{
		"ItemId":     itemID,
		"Categories": categories,
		"Labels":     labels,
		"Comment":    comment,
		"IsHidden":   false,
	}
	_, err := c.do(ctx, http.MethodPost, "/api/items", []map[string]any{payload})
	if err == nil {
		return nil
	}
	_, err2 := c.do(ctx, http.MethodPost, "/api/item", payload)
	if err2 == nil {
		return nil
	}
	return err
}

func (c *Client) InsertFeedback(ctx context.Context, feedbackType, userID, itemID string, ts time.Time) error {
	record := feedbackRecord{
		FeedbackType: feedbackType,
		UserId:       userID,
		ItemId:       itemID,
	}
	if !ts.IsZero() {
		record.Timestamp = ts.UTC().Format(time.RFC3339)
	}

	_, err := c.do(ctx, http.MethodPut, "/api/feedback", []feedbackRecord{record})
	if err == nil {
		return nil
	}

	_, err = c.do(ctx, http.MethodPost, "/api/feedback", []feedbackRecord{record})
	if err == nil {
		return nil
	}

	fallbackPath := fmt.Sprintf("/api/feedback/%s/%s/%s", url.PathEscape(feedbackType), url.PathEscape(userID), url.PathEscape(itemID))
	_, err2 := c.do(ctx, http.MethodPut, fallbackPath, nil)
	if err2 == nil {
		return nil
	}
	return err
}

func (c *Client) RecommendForUser(ctx context.Context, userID string, n int, offset int) (*RecommendResult, error) {
	if n <= 0 {
		n = 20
	}
	if offset < 0 {
		offset = 0
	}
	q := url.Values{}
	q.Set("n", strconv.Itoa(n))
	q.Set("offset", strconv.Itoa(offset))

	path := fmt.Sprintf("/api/recommend/%s?%s", url.PathEscape(userID), q.Encode())
	body, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var raw []any
	if err := json.Unmarshal(body, &raw); err == nil {
		res := &RecommendResult{UserID: userID, Items: make([]string, 0, len(raw))}
		for _, v := range raw {
			s, ok := v.(string)
			if ok && strings.TrimSpace(s) != "" {
				res.Items = append(res.Items, s)
			}
		}
		return res, nil
	}

	var wrapped struct {
		Items []string `json:"items"`
	}
	if err := json.Unmarshal(body, &wrapped); err == nil {
		return &RecommendResult{UserID: userID, Items: wrapped.Items}, nil
	}

	return &RecommendResult{UserID: userID, Items: []string{}}, nil
}

func (c *Client) do(ctx context.Context, method, path string, payload any) ([]byte, error) {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("gorse %s %s -> %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}

	return data, nil
}
