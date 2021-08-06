package bramble

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractBoundaryIDs(t *testing.T) {
	dataJSON := `{
		"gizmos": [
			{
				"id": "1",
				"name": "Gizmo 1",
				"owner": {
					"_id": "1"
				}
			},
			{
				"id": "2",
				"name": "Gizmo 2",
				"owner": {
					"_id": "1"
				}
			},
			{
				"id": "3",
				"name": "Gizmo 3",
				"owner": {
					"_id": "2"
				}
			}
		]
	}`
	data := map[string]interface{}{}
	expected := []string{"1", "1", "2"}
	insertionPoint := []string{"gizmos", "owner"}
	require.NoError(t, json.Unmarshal([]byte(dataJSON), &data))
	result, err := extractBoundaryIDs(data, insertionPoint)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}
