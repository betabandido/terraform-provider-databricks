package databricks

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/betabandido/databricks-sdk-go/client"
	"github.com/betabandido/databricks-sdk-go/models"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
	"os"
	"strings"
	"testing"
)

func TestAccDatabricksNotebook_basic(t *testing.T) {
	languages := []string{
		"SCALA", "PYTHON", "SQL", "R",
	}

	for _, language := range languages {
		resource.Test(t, resource.TestCase{
			PreCheck:     func() { testAccPreCheck(t) },
			Providers:    testAccProviders,
			CheckDestroy: testAccCheckDatabricksNotebookDestroy,
			Steps: []resource.TestStep{
				{
					Config: testAccDatabricksNotebookConfig(language),
				},
			},
		})
	}
}

func testAccCheckDatabricksNotebookDestroy(s *terraform.State) error {
	endpoint := testAccProvider.Meta().(*Client).workspace

	path := s.RootModule().Resources["databricks_notebook.notebook"].Primary.ID

	_, err := endpoint.GetStatus(&models.WorkspaceGetStatusRequest{
		Path: path,
	})

	if err == nil {
		return errors.New("notebook still exists")
	}

	respErr, ok := err.(client.Error)
	if !ok {
		return err
	}

	if respErr.Code() != "RESOURCE_DOES_NOT_EXIST" {
		return err
	}

	return nil
}

func testAccDatabricksNotebookConfig(language string) string {
	commentMark := map[string]string{
		"SCALA":  "//",
		"PYTHON": "#",
		"SQL":    "--",
		"R":      "#",
	}

	const formatStr = `
resource "databricks_notebook" "notebook" {
    path = "%s/tf-test-notebook"
    language = "%s"
    content = "${base64encode("%s foobar")}"
}
`
	return fmt.Sprintf(
		formatStr,
		os.Getenv("DATABRICKS_WORKSPACE"),
		language,
		commentMark,
	)
}

func TestDatabricksNotebook_sanitizeContentFailsIfFirstLineHasWrongContent(t *testing.T) {
	_, err := resourceDatabricksNotebookSanitizeContent(
		databricksNotebookCreateContentFromLines([]string{
			"wrong content",
		}),
	)

	if err == nil {
		t.Fatal("No error was returned when wrong content was given")
	}
}

func TestDatabricksNotebook_sanitizeContentStripsFirstLine(t *testing.T) {
	content, err := resourceDatabricksNotebookSanitizeContent(
		databricksNotebookCreateContentFromLines([]string{
			"# Databricks notebook source",
			"line1",
			"line2",
		}),
	)

	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	decoded, err := base64.StdEncoding.DecodeString(*content)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if string(decoded) != "line1\nline2" {
		t.Fatalf("Wrong content: %s", decoded)
	}
}

func databricksNotebookCreateContentFromLines(lines []string) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Join(lines, "\n")))
}
