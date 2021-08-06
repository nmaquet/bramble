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

func TestBuildBoundaryQueryDocuments(t *testing.T) {
	// f := PlanTestFixture1
	// schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: f.Schema})
	// operation := gqlparser.MustLoadQuery(schema, `{movies {id compTitles(limit: 42) { id title }}}`)
	// plan, err := Plan(&PlanningContext{operation.Operations[0], schema, f.Locations, f.IsBoundary, map[string]*Service{
	// 	"A": {Name: "A", ServiceURL: "A"},
	// 	"B": {Name: "B", ServiceURL: "B"},
	// 	"C": {Name: "C", ServiceURL: "C"},
	// }})
	// require.NoError(t, err)
	// step := plan.RootSteps[0].Then[0].Then[0]
	// ids := []string{"1", "2", "3"}

	// Question for next time: what is the easiest way to get the field names and types for each boundary field?
}
