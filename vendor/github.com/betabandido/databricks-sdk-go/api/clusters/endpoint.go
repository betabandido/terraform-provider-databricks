package clusters

import (
	"encoding/json"
	"github.com/betabandido/databricks-sdk-go/client"
	"github.com/betabandido/databricks-sdk-go/models"
)

type Endpoint struct {
	Client *client.Client
}

func (c *Endpoint) Create(request *models.ClustersCreateRequest) (*models.ClustersCreateResponse, error) {
	bytes, err := c.Client.Query("POST", "clusters/create", request)
	if err != nil {
		return nil, err
	}

	resp := models.ClustersCreateResponse{}
	err = json.Unmarshal(bytes, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *Endpoint) Start(request *models.ClustersStartRequest) error {
	_, err := c.Client.Query("POST", "clusters/start", request)
	return err
}

func (c *Endpoint) Restart(request *models.ClustersRestartRequest) error {
	_, err := c.Client.Query("POST", "clusters/restart", request)
	return err
}

func (c *Endpoint) Delete(request *models.ClustersDeleteRequest) error {
	_, err := c.Client.Query("POST", "clusters/delete", request)
	return err
}

func (c *Endpoint) PermanentDelete(request *models.ClustersPermanentDeleteRequest) error {
	_, err := c.Client.Query("POST", "clusters/permanent-delete", request)
	return err
}

func (c *Endpoint) Get(request *models.ClustersGetRequest) (*models.ClustersGetResponse, error) {
	bytes, err := c.Client.Query("GET", "clusters/get", request)
	if err != nil {
		return nil, err
	}

	resp := models.ClustersGetResponse{}
	err = json.Unmarshal(bytes, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *Endpoint) List() (*models.ClustersListResponse, error) {
	bytes, err := c.Client.Query("GET", "clusters/list", nil)
	if err != nil {
		return nil, err
	}

	resp := models.ClustersListResponse{}
	err = json.Unmarshal(bytes, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
