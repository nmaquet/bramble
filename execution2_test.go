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
			getOwners(ids: [ID!]!): [Owner!]!
		}
	`
	schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})
	boundaryField := BoundaryQuery{Query: "getOwners", Array: true}
	ids := []string{"1", "2", "3"}
	selectionSet := []ast.Selection{
		&ast.Field{
			Alias:            "_id",
			Name:             "id",
			Definition:       schema.Types["Owner"].Fields.ForName("id"),
			ObjectDefinition: schema.Types["Owner"],
		},
		&ast.Field{
			Alias:            "name",
			Name:             "name",
			Definition:       schema.Types["Owner"].Fields.ForName("name"),
			ObjectDefinition: schema.Types["Owner"],
		},
	}
	step := QueryPlanStep{
		ServiceURL:     "http://example.com:8080",
		ServiceName:    "test",
		ParentType:     "Gizmo",
		SelectionSet:   selectionSet,
		InsertionPoint: []string{"gizmos", "owner"},
		Then:           nil,
	}
	expected := []string{`{ _result: getOwners(ids: ["1", "2", "3"]) { _id: id name } }`}
	ctx := testContextWithoutVariables(nil)
	docs, err := buildBoundaryQueryDocuments(ctx, schema, step, ids, boundaryField, 1)
	require.NoError(t, err)
	require.Equal(t, expected, docs)
}

func TestBuildNonArrayBoundaryQueryDocuments(t *testing.T) {
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
			getOwner(id: ID!): Owner!
		}
	`
	schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})
	boundaryField := BoundaryQuery{Query: "getOwner", Array: false}
	ids := []string{"1", "2", "3"}
	selectionSet := []ast.Selection{
		&ast.Field{
			Alias:            "_id",
			Name:             "id",
			Definition:       schema.Types["Owner"].Fields.ForName("id"),
			ObjectDefinition: schema.Types["Owner"],
		},
		&ast.Field{
			Alias:            "name",
			Name:             "name",
			Definition:       schema.Types["Owner"].Fields.ForName("name"),
			ObjectDefinition: schema.Types["Owner"],
		},
	}
	step := QueryPlanStep{
		ServiceURL:     "http://example.com:8080",
		ServiceName:    "test",
		ParentType:     "Gizmo",
		SelectionSet:   selectionSet,
		InsertionPoint: []string{"gizmos", "owner"},
		Then:           nil,
	}
	expected := []string{`{ _0: getOwner(id: "1") { _id: id name } _1: getOwner(id: "2") { _id: id name } _2: getOwner(id: "3") { _id: id name } }`}
	ctx := testContextWithoutVariables(nil)
	docs, err := buildBoundaryQueryDocuments(ctx, schema, step, ids, boundaryField, 10)
	require.NoError(t, err)
	require.Equal(t, expected, docs)
}

func TestBuildBatchedNonArrayBoundaryQueryDocuments(t *testing.T) {
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
			getOwner(id: ID!): Owner!
		}
	`
	schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})
	boundaryField := BoundaryQuery{Query: "getOwner", Array: false}
	ids := []string{"1", "2", "3"}
	selectionSet := []ast.Selection{
		&ast.Field{
			Alias:            "_id",
			Name:             "id",
			Definition:       schema.Types["Owner"].Fields.ForName("id"),
			ObjectDefinition: schema.Types["Owner"],
		},
		&ast.Field{
			Alias:            "name",
			Name:             "name",
			Definition:       schema.Types["Owner"].Fields.ForName("name"),
			ObjectDefinition: schema.Types["Owner"],
		},
	}
	step := QueryPlanStep{
		ServiceURL:     "http://example.com:8080",
		ServiceName:    "test",
		ParentType:     "Gizmo",
		SelectionSet:   selectionSet,
		InsertionPoint: []string{"gizmos", "owner"},
		Then:           nil,
	}
	expected := []string{`{ _0: getOwner(id: "1") { _id: id name } _1: getOwner(id: "2") { _id: id name } }`, `{ _2: getOwner(id: "3") { _id: id name } }`}
	ctx := testContextWithoutVariables(nil)
	docs, err := buildBoundaryQueryDocuments(ctx, schema, step, ids, boundaryField, 2)
	require.NoError(t, err)
	require.Equal(t, expected, docs)
}

func TestMergeExecutionResults(t *testing.T) {
	t.Run("merges single map", func(t *testing.T) {
		inputMap := jsonToInterfaceMap(`{
			"gizmo": {
				"id": "1",
				"color": "Gizmo A"
			}
		}`)

		result := ExecutionResult{
			ServiceURL:     "http://service-a",
			InsertionPoint: []string{},
			Data:           inputMap,
		}

		mergedMap, err := mergeExecutionResults([]ExecutionResult{result})

		require.NoError(t, err)
		require.Equal(t, inputMap, mergedMap)
	})

	t.Run("merges two top level results", func(t *testing.T) {
		inputMapA := jsonToInterfaceMap(`{
			"gizmoA": {
				"id": "1",
				"color": "Gizmo A"
			}
		}`)

		resultA := ExecutionResult{
			ServiceURL:     "http://service-a",
			InsertionPoint: []string{},
			Data:           inputMapA,
		}

		inputMapB := jsonToInterfaceMap(`{
			"gizmoB": {
				"id": "2",
				"color": "Gizmo B"
			}
		}`)

		resultB := ExecutionResult{
			ServiceURL:     "http://service-b",
			InsertionPoint: []string{},
			Data:           inputMapB,
		}

		mergedMap, err := mergeExecutionResults([]ExecutionResult{resultA, resultB})

		expected := jsonToInterfaceMap(`{
			"gizmoA": {
				"id": "1",
				"color": "Gizmo A"
			},
			"gizmoB": {
				"id": "2",
				"color": "Gizmo B"
			}
		}`)

		require.NoError(t, err)
		require.Equal(t, expected, mergedMap)
	})

	t.Run("merges root step with child step (root step returns object, boundary field is non array)", func(t *testing.T) {
		inputMapA := jsonToInterfaceMap(`{
			"gizmo": {
				"id": "1",
				"color": "Gizmo A",
				"owner": {
					"_id": "1"
				}
			}
		}`)

		resultA := ExecutionResult{
			ServiceURL:     "http://service-a",
			InsertionPoint: []string{},
			Data:           inputMapA,
		}

		inputMapB := jsonToInterfaceMap(`{
			"_0": {
				"_id": "1",
				"name": "Owner A"
			}
		}`)

		resultB := ExecutionResult{
			ServiceURL:     "http://service-b",
			InsertionPoint: []string{"gizmo", "owner"},
			Data:           inputMapB,
		}

		mergedMap, err := mergeExecutionResults([]ExecutionResult{resultA, resultB})

		expected := jsonToInterfaceMap(`{
			"gizmo": {
				"id": "1",
				"color": "Gizmo A",
				"owner": {
					"_id": "1",
					"name": "Owner A"
				}
			}
		}`)

		require.NoError(t, err)
		require.Equal(t, expected, mergedMap)
	})

	t.Run("merges root step with child step (root step returns array, boundary field is non array)", func(t *testing.T) {
		inputMapA := jsonToInterfaceMap(`{
			"gizmos": [
				{
					"id": "1",
					"color": "RED",
					"owner": {
						"_id": "4"
					}
				},
				{
					"id": "2",
					"color": "GREEN",
					"owner": {
						"_id": "5"
					}
				},
				{
					"id": "3",
					"color": "BLUE",
					"owner": {
						"_id": "6"
					}
				}
			]
		}`)

		resultA := ExecutionResult{
			ServiceURL:     "http://service-a",
			InsertionPoint: []string{},
			Data:           inputMapA,
		}

		inputMapB := jsonToInterfaceMap(`{
			"_0": {
				"_id": "4",
				"name": "Owner A"
			},
			"_1": {
				"_id": "5",
				"name": "Owner B"
			},
			"_2": {
				"_id": "6",
				"name": "Owner C"
			}
		}`)

		resultB := ExecutionResult{
			ServiceURL:     "http://service-b",
			InsertionPoint: []string{"gizmos", "owner"},
			Data:           inputMapB,
		}

		mergedMap, err := mergeExecutionResults([]ExecutionResult{resultA, resultB})

		expected := jsonToInterfaceMap(`{
			"gizmos": [
				{
					"id": "1",
					"color": "RED",
					"owner": {
						"_id": "4",
						"name": "Owner A"
					}
				},
				{
					"id": "2",
					"color": "GREEN",
					"owner": {
						"_id": "5",
						"name": "Owner B"
					}
				},
				{
					"id": "3",
					"color": "BLUE",
					"owner": {
						"_id": "6",
						"name": "Owner C"
					}
				}
			]
		}`)

		require.NoError(t, err)
		require.Equal(t, expected, mergedMap)
	})
}

func jsonToInterfaceMap(jsonString string) map[string]interface{} {
	var outputMap map[string]interface{}
	err := json.Unmarshal([]byte(jsonString), &outputMap)
	if err != nil {
		panic(err)
	}

	return outputMap
}
