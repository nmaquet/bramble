package bramble

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
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
	ddl := `
		type Gizmo {
			id: ID!
			color: String!
			owner: Owner
		}

		type Owner {
			id: ID!
			name: String!
		}

		type Query {
			gizmos: [Gizmo!]!
		}
	`
	schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})
	boundaryField := BoundaryQuery{Query: "getOwners", Array: true}
	ids := []string{"1", "2", "3"}
	selectionSet := getSelectionSet(schema, "query { getOwners { id name } }")
	step := QueryPlanStep{
		ServiceURL:     "http://example.com:8080",
		ServiceName:    "test",
		ParentType:     "Gizmo",
		SelectionSet:   selectionSet,
		InsertionPoint: []string{"gizmos", "owner"},
		Then:           nil,
	}
	expected := []string{"TBD"}
	docs, err := buildBoundaryQueryDocuments(step, ids, boundaryField)
	require.NoError(t, err)
	require.Equal(t, expected, docs)
}

func getSelectionSet(schema *ast.Schema, query string) ast.SelectionSet {
	return gqlparser.MustLoadQuery(schema, query).Operations[0].SelectionSet
}
