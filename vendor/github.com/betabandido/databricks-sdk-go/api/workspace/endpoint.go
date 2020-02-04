package workspace

import (
	"encoding/json"
	"github.com/tcz001/databricks-sdk-go/client"
	"github.com/tcz001/databricks-sdk-go/models"
)

type Endpoint struct {
	Client *client.Client
}

func (w *Endpoint) Delete(request *models.WorkspaceDeleteRequest) error {
	_, err := w.Client.Query("POST", "workspace/delete", request)
	return err
}

func (w *Endpoint) Export(request *models.WorkspaceExportRequest) (*models.WorkspaceExportResponse, error) {
	bytes, err := w.Client.Query("GET", "workspace/export", request)
	if err != nil {
		return nil, err
	}

	resp := models.WorkspaceExportResponse{}
	err = json.Unmarshal(bytes, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (w *Endpoint) GetStatus(request *models.WorkspaceGetStatusRequest) (*models.WorkspaceGetStatusResponse, error) {
	bytes, err := w.Client.Query("GET", "workspace/get-status", request)
	if err != nil {
		return nil, err
	}

	resp := models.WorkspaceGetStatusResponse{}
	err = json.Unmarshal(bytes, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (w *Endpoint) Import(request *models.WorkspaceImportRequest) error {
	if request.Language == nil {
		defaultLanguage := models.SCALA
		request.Language = &defaultLanguage
	}

	if request.Format == nil {
		defaultFormat := models.SOURCE
		request.Format = &defaultFormat
	}

	_, err := w.Client.Query("POST", "workspace/import", request)
	return err
}

func (w *Endpoint) List(request *models.WorkspaceListRequest) (*models.WorkspaceListResponse, error) {
	bytes, err := w.Client.Query("GET", "workspace/list", request)
	if err != nil {
		return nil, err
	}

	resp := models.WorkspaceListResponse{}
	err = json.Unmarshal(bytes, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (w *Endpoint) Mkdirs(request *models.WorkspaceMkdirsRequest) error {
	_, err := w.Client.Query("POST", "workspace/mkdirs", request)
	return err
}
