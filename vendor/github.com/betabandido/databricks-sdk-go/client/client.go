package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/betabandido/databricks-sdk-go/models"
	"github.com/golang/glog"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Options struct {
	Domain *string
	Token  *string
}

type Client struct {
	http    *http.Client
	baseUrl *url.URL
	header  http.Header
}

func NewClient(opts Options) (*Client, error) {
	loadEnvConfig(&opts)

	if opts.Domain == nil || opts.Token == nil {
		return nil, fmt.Errorf("missing credentials")
	}

	baseUrl, err := url.Parse(fmt.Sprintf("https://%s/api/2.0/", *opts.Domain))
	if err != nil {
		panic(err)
	}

	header := http.Header{}
	header.Add("Authorization", fmt.Sprintf("Bearer %s", *opts.Token))

	client := Client{
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseUrl: baseUrl,
		header:  header,
	}

	return &client, nil
}

func loadEnvConfig(opts *Options) {
	if opts.Domain == nil {
		if v := os.Getenv("DATABRICKS_DOMAIN"); v != "" {
			opts.Domain = &v
		}
	}

	if opts.Token == nil {
		if v := os.Getenv("DATABRICKS_TOKEN"); v != "" {
			opts.Token = &v
		}
	}
}

func (c *Client) Query(method string, path string, data interface{}) ([]byte, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	queryUrl := c.baseUrl.ResolveReference(u)

	var body []byte = nil
	if data != nil {
		body, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
	}

	request, err := http.NewRequest(method, queryUrl.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	request.Header = c.header

	glog.Infof("HTTP request: %v", request)

	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}

	glog.Infof("HTTP response: %v", response)

	defer response.Body.Close()

	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	glog.Infof("Response bytes: %s", responseBytes)

	if response.StatusCode != 200 {
		if strings.Contains(response.Header.Get("Content-Type"), "json") {
			errorResponse := models.ErrorResponse{}
			err = json.Unmarshal(responseBytes, &errorResponse)
			if err != nil {
				return nil, err
			}
			return nil, Error{ErrorResponse: errorResponse}
		} else {
			return nil, fmt.Errorf("request error: %s", string(responseBytes))
		}
	}

	return responseBytes, nil
}

type Error struct {
	ErrorResponse models.ErrorResponse
}

func (e Error) Error() string {
	return e.ErrorResponse.Message
}

func (e Error) Code() string {
	return e.ErrorResponse.ErrorCode
}
