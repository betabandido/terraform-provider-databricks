package databricks

import (
	"github.com/nenetto/databricks-sdk-go/api/clusters"
	"github.com/nenetto/databricks-sdk-go/api/instanceprofile"
	"github.com/nenetto/databricks-sdk-go/api/workspace"
	apiClient "github.com/nenetto/databricks-sdk-go/client"
	"time"
)

const (
	maxRetries = 1
	retryDelay = 5 * time.Second
)

type Config struct {
	Domain *string
	Token  *string
}

type Client struct {
	clusters         *clusters.Endpoint
	workspace        *workspace.Endpoint
	instanceprofile  *instanceprofile.Endpoint
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
	client.instanceprofile = &instanceprofile.Endpoint{Client: cl}

	return &client, nil
}
