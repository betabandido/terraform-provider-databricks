package databricks

import (
	"time"

	"github.com/tcz001/databricks-sdk-go/api/clusters"
	secrets "github.com/tcz001/databricks-sdk-go/api/secrets"
	"github.com/tcz001/databricks-sdk-go/api/workspace"
	apiClient "github.com/tcz001/databricks-sdk-go/client"
)

const (
	maxRetries = 3
	retryDelay = 5 * time.Second
)

type Config struct {
	Domain *string
	Token  *string
}

type Client struct {
	clusters  *clusters.Endpoint
	workspace *workspace.Endpoint
	secrets   *secrets.Endpoint
}

func (c *Config) Client() (interface{}, error) {
	var client Client

	opts := apiClient.Options{
		Domain:     c.Domain,
		Token:      c.Token,
		MaxRetries: maxRetries,
		RetryDelay: retryDelay,
	}
	cl, err := apiClient.NewClient(opts)
	if err != nil {
		return nil, err
	}

	client.clusters = &clusters.Endpoint{Client: cl}
	client.workspace = &workspace.Endpoint{Client: cl}
	client.secrets = &secrets.Endpoint{Client: cl}

	return &client, nil
}
