package clusters

import (
	"encoding/json"
	"fmt"
	"github.com/tcz001/databricks-sdk-go/client"
	"github.com/tcz001/databricks-sdk-go/models"
	"time"
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

func (c *Endpoint) CreateSync(request *models.ClustersCreateRequest) (
	resp *models.ClustersCreateResponse,
	err error,
) {
	opFunc := func() (*string, error) {
		var err error
		resp, err = c.Create(request)
		if err != nil {
			return nil, err
		}
		return &resp.ClusterId, nil
	}

	err = c.executeSync(opFunc, models.RUNNING, []models.ClustersClusterState{
		models.PENDING,
	})

	return
}

func (c *Endpoint) Edit(request *models.ClustersEditRequest) error {
	_, err := c.Client.Query("POST", "clusters/edit", request)
	return err
}

func (c *Endpoint) EditSync(request *models.ClustersEditRequest) error {
	opFunc := func() (*string, error) { return &request.ClusterId, c.Edit(request) }

	state, err := c.getState(request.ClusterId)
	if err != nil {
		return err
	}

	if *state == models.TERMINATED {
		return nil
	}

	return c.executeSync(opFunc, models.RUNNING, []models.ClustersClusterState{
		models.RESTARTING,
	})
}

func (c *Endpoint) Start(request *models.ClustersStartRequest) error {
	_, err := c.Client.Query("POST", "clusters/start", request)
	return err
}

func (c *Endpoint) StartSync(request *models.ClustersStartRequest) error {
	opFunc := func() (*string, error) { return &request.ClusterId, c.Start(request) }
	return c.executeSync(opFunc, models.RUNNING, []models.ClustersClusterState{models.PENDING})
}

func (c *Endpoint) Restart(request *models.ClustersRestartRequest) error {
	_, err := c.Client.Query("POST", "clusters/restart", request)
	return err
}

func (c *Endpoint) RestartSync(request *models.ClustersRestartRequest) error {
	opFunc := func() (*string, error) { return &request.ClusterId, c.Restart(request) }
	return c.executeSync(opFunc, models.RUNNING, []models.ClustersClusterState{models.RESTARTING})
}

func (c *Endpoint) Delete(request *models.ClustersDeleteRequest) error {
	_, err := c.Client.Query("POST", "clusters/delete", request)
	return err
}

func (c *Endpoint) DeleteSync(request *models.ClustersDeleteRequest) error {
	opFunc := func() (*string, error) { return &request.ClusterId, c.Delete(request) }
	return c.executeSync(opFunc, models.TERMINATED, []models.ClustersClusterState{
		models.PENDING,
		models.RESTARTING,
		models.RESIZING,
		models.TERMINATING,
	})
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

func (c *Endpoint) executeSync(
	opFunc func() (*string, error),
	state models.ClustersClusterState,
	validStates []models.ClustersClusterState,
) error {
	clusterId, err := opFunc()
	if err != nil {
		return err
	}

	validStatesMap := make(map[models.ClustersClusterState]bool, len(validStates))
	for _, v := range validStates {
		validStatesMap[v] = true
	}

	endTime := time.Now().Add(30 * time.Minute)

	for time.Now().Before(endTime) {
		currState, err := c.getState(*clusterId)
		if err != nil {
			return err
		}

		if *currState == state {
			return nil
		}

		if _, ok := validStatesMap[*currState]; !ok {
			return fmt.Errorf("unexpected state (%s) for cluster %s", *currState, *clusterId)
		}

		time.Sleep(10 * time.Second)
	}

	return fmt.Errorf("timeout when waiting for cluster %s to have state %s", *clusterId, state)
}

func (c *Endpoint) getState(clusterId string) (*models.ClustersClusterState, error) {
	req := models.ClustersGetRequest{ClusterId: clusterId}
	resp, err := c.Get(&req)
	if err != nil {
		return nil, err
	}

	return resp.State, nil
}
