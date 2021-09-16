package bramble

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func TestQueryWithNamespace(t *testing.T) {
	f := &queryExecution2Fixture{
		services: []testService{
			{
				schema: `
				directive @namespace on OBJECT

				type NamespacedMovie {
					id: ID!
					title: String
				}

				type NamespaceQuery @namespace {
					movie(id: ID!): NamespacedMovie!
				}

				type Query {
					namespace: NamespaceQuery!
				}
				`,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Write([]byte(`{
						"data": {
							"namespace": {
								"movie": {
									"id": "1",
									"title": "Test title"
								}
							}
						}
					}`))
				}),
			},
		},
		query: `{
			namespace {
				movie(id: "1") {
					id
					title
				}
				__typename
			}
		}`,
		expected: `{
			"namespace": {
				"movie": {
					"id": "1",
					"title": "Test title"
				},
				"__typename": "NamespaceQuery"
			}
		}`,
	}

	f.checkSuccess(t)
}

func TestQuery2Error(t *testing.T) {
	f := &queryExecution2Fixture{
		services: []testService{
			{
				schema: `type Movie {
					id: ID!
					title: String
				}

				type Query {
					movie(id: ID!): Movie!
				}
				`,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Write([]byte(`{
						"errors": [
							{
								"message": "Movie does not exist",
								"path": ["movie"],
								"extensions": {
									"code": "NOT_FOUND"
								}
							}
						]
					}`))
				}),
			},
		},
		query: `{
			movie(id: "1") {
				id
				title
			}
		}`,
		errors: gqlerror.List{
			&gqlerror.Error{
				Message: "Movie does not exist",
				Path:    ast.Path{ast.PathName("movie")},
				Locations: []gqlerror.Location{
					{Line: 2, Column: 4},
				},
				Extensions: map[string]interface{}{
					"code":         "NOT_FOUND",
					"selectionSet": `{ movie(id: "1") { id title } }`,
					"serviceName":  "",
				},
			},
			&gqlerror.Error{
				Message: `got a null response for non-nullable field "movie"`,
			},
		},
	}

	f.run(t)
}

func TestFederatedQuery2FragmentSpreads(t *testing.T) {
	serviceA := testService{
		schema: `
		directive @boundary on OBJECT
		interface Snapshot {
			id: ID!
			name: String!
		}

		type Gizmo @boundary {
			id: ID!
		}

		type Gadget @boundary {
			id: ID!
		}

		type GizmoImplementation implements Snapshot {
			id: ID!
			name: String!
			gizmos: [Gizmo!]!
		}

		type GadgetImplementation implements Snapshot {
			id: ID!
			name: String!
			gadgets: [Gadget!]!
		}

		type Query {
			snapshot(id: ID!): Snapshot!
		}`,
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if strings.Contains(string(body), "GIZMO1") {
				w.Write([]byte(`
				{
					"data": {
						"snapshot": {
							"id": "100",
							"name": "foo",
							"gizmos": [{ "id": "GIZMO1" }],
							"__typename": "GizmoImplementation"
						}
					}
				}`))
			} else {
				w.Write([]byte(`
				{
					"data": {
						"snapshot": {
							"id": "100",
							"name": "foo",
							"gadgets": [{ "id": "GADGET1" }],
							"__typename": "GadgetImplementation"
						}
					}
				}`))

			}
		}),
	}

	serviceB := testService{
		schema: `
		directive @boundary on OBJECT
		type Gizmo @boundary {
			id: ID!
			name: String!
		}

		type Agent {
			name: String!
			country: String!
		}

		type Gadget @boundary {
			id: ID!
			name: String!
			agents: [Agent!]!
		}

		type Query {
			gizmo(id: ID!): Gizmo @boundary
			gadgets(id: [ID!]!): [Gadget!]! @boundary
		}`,
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if strings.Contains(string(body), "GIZMO1") {
				w.Write([]byte(`
				{
					"data": {
						"_0": {
							"id": "GIZMO1",
							"name": "Gizmo #1"
						}
					}
				}`))
			} else {
				w.Write([]byte(`
				{
					"data": {
						"_result": [
							{
								"id": "GADGET1",
								"name": "Gadget #1",
								"agents": [
									{
										"name": "James Bond",
										"country": "UK",
										"__typename": "Agent"
									}
								]
							}
						]
					}
				}`))
			}
		}),
	}

	t.Run("with inline fragment spread", func(t *testing.T) {
		f := &queryExecution2Fixture{
			services: []testService{serviceA, serviceB},
			query: `
			query Foo {
				snapshot(id: "GIZMO1") {
					id
					name
					... on GizmoImplementation {
						gizmos {
							id
							name
						}
					}
				}
			}`,
			expected: `
			{
				"snapshot": {
					"id": "100",
					"name": "foo",
					"gizmos": [{ "id": "GIZMO1", "name": "Gizmo #1" }]
				}
			}`,
		}

		f.checkSuccess(t)
	})

	t.Run("with named fragment spread", func(t *testing.T) {
		f := &queryExecution2Fixture{
			services: []testService{serviceA, serviceB},
			query: `
			query Foo {
				snapshot(id: "GIZMO1") {
					id
					name
					... NamedFragment
				}
			}

			fragment NamedFragment on GizmoImplementation {
				gizmos {
					id
					name
				}
			}`,
			expected: `
			{
				"snapshot": {
					"id": "100",
					"name": "foo",
					"gizmos": [{ "id": "GIZMO1", "name": "Gizmo #1" }]
				}
			}`,
		}

		f.checkSuccess(t)
	})

	t.Run("with nested fragment spread", func(t *testing.T) {
		f := &queryExecution2Fixture{
			services: []testService{serviceA, serviceB},
			query: `
			query Foo {
				snapshot(id: "GIZMO1") {
					... NamedFragment
				}
			}

			fragment NamedFragment on Snapshot {
				id
				name
				... on GizmoImplementation {
					gizmos {
						id
						name
				  	}
				}
			}`,
			expected: `
			{
				"snapshot": {
					"id": "100",
					"name": "foo",
					"gizmos": [{ "id": "GIZMO1", "name": "Gizmo #1" }]
				}
			}`,
		}

		f.checkSuccess(t)
	})

	t.Run("with multiple implementation fragment spreads (gizmo implementation)", func(t *testing.T) {
		f := &queryExecution2Fixture{
			services: []testService{serviceA, serviceB},
			query: `
			query {
				snapshot(id: "GIZMO1") {
					id
					... NamedFragment
				}
			}

			fragment NamedFragment on Snapshot {
				name
				... on GizmoImplementation {
					gizmos {
						id
						name
				  	}
				}
				... on GadgetImplementation {
					gadgets {
						id
						name
				  	}
				}
			}`,
			expected: `
			{
				"snapshot": {
					"id": "100",
					"name": "foo",
					"gizmos": [{ "id": "GIZMO1", "name": "Gizmo #1" }]
				}
			}`,
		}

		f.checkSuccess(t)
	})

	t.Run("with multiple implementation fragment spreads (gadget implementation)", func(t *testing.T) {
		f := &queryExecution2Fixture{
			services: []testService{serviceA, serviceB},
			query: `
			query Foo {
				snapshot(id: "GADGET1") {
					... NamedFragment
				}
			}

			fragment GadgetFragment on GadgetImplementation {
				gadgets {
					id
					name
					agents {
						name
						... on Agent {
							country
						}
					}
				}
			}

			fragment NamedFragment on Snapshot {
				id
				name
				... on GizmoImplementation {
					gizmos {
						id
						name
				  	}
				}
				... GadgetFragment
			}`,
			expected: `
			{
				"snapshot": {
					"id": "100",
					"name": "foo",
					"gadgets": [
						{
							"id": "GADGET1",
							"name": "Gadget #1",
							"agents": [
								{"name": "James Bond", "country": "UK"}
							]
						}
					]
				}
			}`,
		}

		f.checkSuccess(t)
	})

}

func TestQueryExecution2MultipleServices(t *testing.T) {
	f := &queryExecution2Fixture{
		services: []testService{
			{
				schema: `directive @boundary on OBJECT
				type Movie @boundary {
					id: ID!
					title: String
				}

				type Query {
					movie(id: ID!): Movie!
				}`,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Write([]byte(`{
						"data": {
							"movie": {
								"id": "1",
								"title": "Test title"
							}
						}
					}
					`))
				}),
			},
			{
				schema: `directive @boundary on OBJECT | FIELD_DEFINITION

				type Movie @boundary {
					id: ID!
					release: Int
				}

				type Query {
					movie(id: ID!): Movie! @boundary
				}`,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Write([]byte(`{
						"data": {
							"_0": {
								"id": "1",
								"release": 2007
							}
						}
					}
					`))
				}),
			},
		},
		query: `{
			movie(id: "1") {
				id
				title
				release
			}
		}`,
		expected: `{
			"movie": {
				"id": "1",
				"title": "Test title",
				"release": 2007
			}
		}`,
	}

	f.checkSuccess(t)
}

func TestQueryExecution2WithNullResponse(t *testing.T) {
	f := &queryExecution2Fixture{
		services: []testService{
			{
				schema: `directive @boundary on OBJECT
				type Movie @boundary {
					id: ID!
				}

				type Query {
					movies: [Movie!]
				}`,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Write([]byte(`{
						"data": {
							"movies": null
						}
					}
					`))
				}),
			},
			{
				schema: `directive @boundary on OBJECT | FIELD_DEFINITION
				interface Node { id: ID! }

				type Movie @boundary {
					id: ID!
					title: String
				}

				type Query {
					movie(id: ID!): Movie! @boundary
				}`,
				handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					assert.Fail(t, "handler should not be called")
				}),
			},
		},
		query: `{
			movies {
				id
				title
			}
		}`,
		expected: `{
			"movies": null
		}`,
	}

	f.checkSuccess(t)
}

func TestQueryExecution2WithSingleService(t *testing.T) {
	f := &queryExecution2Fixture{
		services: []testService{
			{
				schema: `type Movie {
					id: ID!
					title: String
				}

				type Query {
					movie(id: ID!): Movie!
				}
				`,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Write([]byte(`{
						"data": {
							"movie": {
								"id": "1",
								"title": "Test title"
							}
						}
					}`))
				}),
			},
		},
		query: `{
			movie(id: "1") {
				id
				title
			}
		}`,
		expected: `{
			"movie": {
				"id": "1",
				"title": "Test title"
			}
		}`,
	}

	f.checkSuccess(t)
}

func TestQueryWithArrayBoundaryFieldsAndMultipleChildrenSteps2(t *testing.T) {
	f := &queryExecution2Fixture{
		services: []testService{
			{
				schema: `directive @boundary on OBJECT | FIELD_DEFINITION

				type Movie @boundary {
					id: ID!
					title: String
				}

				type Query {
					randomMovie: Movie!
					movies(ids: [ID!]!): [Movie]! @boundary
				}`,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					b, _ := io.ReadAll(r.Body)
					if strings.Contains(string(b), "randomMovie") {
						w.Write([]byte(`{
						"data": {
							"randomMovie": {
									"id": "1",
									"title": "Movie 1"
							}
						}
					}
					`))
					} else {
						w.Write([]byte(`{
						"data": {
							"_result": [
								{ "id": "2", "title": "Movie 2" },
								{ "id": "3", "title": "Movie 3" },
								{ "id": "4", "title": "Movie 4" }
							]
						}
					}
					`))
					}
				}),
			},
			{
				schema: `directive @boundary on OBJECT | FIELD_DEFINITION

				type Movie @boundary {
					id: ID!
					compTitles: [Movie!]!
				}

				type Query {
					movies(ids: [ID!]): [Movie]! @boundary
				}`,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Write([]byte(`{
						"data": {
							"_result": [
								{
									"_id": "1",
									"compTitles": [
										{"id": "2"},
										{"id": "3"},
										{"id": "4"}
									]
								}
							]
						}
					}
					`))
				}),
			},
		},
		query: `{
			randomMovie {
				id
				title
				compTitles {
					id
					title
				}
			}
		}`,
		expected: `{
			"randomMovie":
				{
					"id": "1",
					"title": "Movie 1",
					"compTitles": [
						{ "id": "2", "title": "Movie 2" },
						{ "id": "3", "title": "Movie 3" },
						{ "id": "4", "title": "Movie 4" }
					]
				}
		}`,
	}

	f.checkSuccess(t)
}

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
					"id": "1"
				}
			},
			{
				"id": "3",
				"name": "Gizmo 3",
				"owner": {
					"_id": "2"
				}
			},
			{
				"id": "4",
				"name": "Gizmo 4",
				"owner": {
					"id": "5"
				}
			}
		]
	}`
	data := map[string]interface{}{}
	expected := []string{"1", "1", "2", "5"}
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

		inputSliceB := jsonToInterfaceSlice(`[
			{
				"_id": "1",
				"name": "Owner A"
			}
		]`)

		resultB := ExecutionResult{
			ServiceURL:     "http://service-b",
			InsertionPoint: []string{"gizmo", "owner"},
			Data:           inputSliceB,
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

		inputSliceB := jsonToInterfaceSlice(`[
			{
				"_id": "4",
				"name": "Owner A"
			},
			{
				"_id": "5",
				"name": "Owner B"
			},
			{
				"_id": "6",
				"name": "Owner C"
			}
		]`)

		resultB := ExecutionResult{
			ServiceURL:     "http://service-b",
			InsertionPoint: []string{"gizmos", "owner"},
			Data:           inputSliceB,
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

	t.Run("merges root step with child step (root step returns array, boundary field is array)", func(t *testing.T) {
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

		inputSliceB := jsonToInterfaceSlice(`[
			{
				"_id": "4",
				"name": "Owner A"
			},
			{
				"_id": "5",
				"name": "Owner B"
			},
			{
				"_id": "6",
				"name": "Owner C"
			}
		]`)

		resultB := ExecutionResult{
			ServiceURL:     "http://service-b",
			InsertionPoint: []string{"gizmos", "owner"},
			Data:           inputSliceB,
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

	t.Run("allows using both 'id' and '_id'", func(t *testing.T) {
		inputMapA := jsonToInterfaceMap(`{
			"gizmos": [
				{
					"id": "1",
					"color": "RED",
					"owner": {
						"id": "4"
					}
				},
				{
					"id": "2",
					"color": "GREEN",
					"owner": {
						"id": "5"
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

		inputSliceB := jsonToInterfaceSlice(`[
			{
				"_id": "4",
				"name": "Owner A"
			},
			{
				"id": "5",
				"name": "Owner B"
			},
			{
				"id": "6",
				"name": "Owner C"
			}
		]`)

		resultB := ExecutionResult{
			ServiceURL:     "http://service-b",
			InsertionPoint: []string{"gizmos", "owner"},
			Data:           inputSliceB,
		}

		mergedMap, err := mergeExecutionResults([]ExecutionResult{resultA, resultB})

		expected := jsonToInterfaceMap(`{
			"gizmos": [
				{
					"id": "1",
					"color": "RED",
					"owner": {
						"id": "4",
						"name": "Owner A"
					}
				},
				{
					"id": "2",
					"color": "GREEN",
					"owner": {
						"id": "5",
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

func TestBubbleUpNullValuesInPlace(t *testing.T) {
	t.Run("no expected or unexpected nulls", func(t *testing.T) {
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
		}`

		result := jsonToInterfaceMap(`
			{
				"gizmos": [
					{ "id": "GIZMO1" },
					{ "id": "GIZMO2" },
					{ "id": "GIZMO3" }
				]
			}
		`)

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				gizmos {
					id
				}
			}`

		document := gqlparser.MustLoadQuery(schema, query)
		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.Nil(t, errs)
	})

	t.Run("1 expected null (bubble to root)", func(t *testing.T) {
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
		}`

		result := jsonToInterfaceMap(`
			{
				"gizmos": [
					{ "id": "GIZMO1", "color": "RED" },
					{ "id": "GIZMO2", "color": "GREEN" },
					{ "id": "GIZMO3", "color": null }
				]
			}
		`)

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				gizmos {
					id
					color
				}
			}`

		document := gqlparser.MustLoadQuery(schema, query)
		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.Equal(t, errNullBubbledToRoot, err)
		require.Nil(t, errs)
	})

	t.Run("1 expected null (bubble to middle)", func(t *testing.T) {
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
			gizmos: [Gizmo!]
			getOwners(ids: [ID!]!): [Owner!]!
		}`

		result := jsonToInterfaceMap(`
			{
				"gizmos": [
					{ "id": "GIZMO1", "color": "RED" },
					{ "id": "GIZMO2", "color": "GREEN" },
					{ "id": "GIZMO3", "color": null }
				]
			}
		`)

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				gizmos {
					id
					color
				}
			}`

		document := gqlparser.MustLoadQuery(schema, query)
		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.Equal(t, GraphqlErrors([]GraphqlError{{Message: "field failed to resolve", Path: ast.Path{ast.PathName("gizmos"), ast.PathIndex(2), ast.PathName("color")}, Extensions: nil}}), errs)
		require.Equal(t, jsonToInterfaceMap(`{ "gizmos": null }`), result)
	})

	t.Run("1 expected null (bubble to middle in array)", func(t *testing.T) {
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
			gizmos: [Gizmo]!
			getOwners(ids: [ID!]!): [Owner!]!
		}`

		result := jsonToInterfaceMap(`
			{
				"gizmos": [
					{ "id": "GIZMO1", "color": "RED" },
					{ "id": "GIZMO3", "color": null },
					{ "id": "GIZMO2", "color": "GREEN" }
				]
			}
		`)

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				gizmos {
					id
					color
				}
			}`

		document := gqlparser.MustLoadQuery(schema, query)
		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.Equal(t, GraphqlErrors([]GraphqlError{{Message: "field failed to resolve", Path: ast.Path{ast.PathName("gizmos"), ast.PathIndex(1), ast.PathName("color")}, Extensions: nil}}), errs)
		require.Equal(t, jsonToInterfaceMap(`{ "gizmos": [ { "id": "GIZMO1", "color": "RED" }, null, { "id": "GIZMO2", "color": "GREEN" } ]	}`), result)
	})

	t.Run("0 expected nulls", func(t *testing.T) {
		ddl := `
		type Gizmo {
			id: ID!
			color: String
			owner: Owner
		}

		type Owner {
			id: ID!
			name: String!
		}

		type Query {
			gizmos: [Gizmo!]!
			getOwners(ids: [ID!]!): [Owner!]!
		}`

		resultJSON := `{
			"gizmos": [
				{ "id": "GIZMO1", "color": "RED" },
				{ "id": "GIZMO2", "color": "GREEN" },
				{ "id": "GIZMO3", "color": null }
			]
		}`

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				gizmos {
					id
					color
				}
			}`

		document := gqlparser.MustLoadQuery(schema, query)
		result := jsonToInterfaceMap(resultJSON)
		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.Empty(t, errs)
		require.Equal(t, jsonToInterfaceMap(resultJSON), result)
	})

	t.Run("works with fragment spreads", func(t *testing.T) {
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
			gizmos: [Gizmo]!
			getOwners(ids: [ID!]!): [Owner!]!
		}`

		resultJSON := `{
			"gizmos": [
				{ "id": "GIZMO1", "color": "RED", "__typename": "Gizmo" },
				{ "id": "GIZMO2", "color": "GREEN", "__typename": "Gizmo" },
				{ "id": "GIZMO3", "color": null, "__typename": "Gizmo" }
			]
		}`

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			fragment GizmoDetails on Gizmo {
				id
				color
				__typename
			}
			{
				gizmos {
					...GizmoDetails
				}
			}
		`

		document := gqlparser.MustLoadQuery(schema, query)

		result := jsonToInterfaceMap(resultJSON)

		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.Equal(t, GraphqlErrors([]GraphqlError{{Message: "field failed to resolve", Path: ast.Path{ast.PathName("gizmos"), ast.PathIndex(2), ast.PathName("color")}, Extensions: nil}}), errs)
		require.Equal(t, jsonToInterfaceMap(`{ "gizmos": [ { "id": "GIZMO1", "color": "RED", "__typename": "Gizmo" }, { "id": "GIZMO2", "color": "GREEN", "__typename": "Gizmo" }, null ]	}`), result)
	})

	t.Run("works with inline fragments", func(t *testing.T) {
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
			gizmos: [Gizmo]!
			getOwners(ids: [ID!]!): [Owner!]!
		}`

		resultJSON := `{
			"gizmos": [
				{ "id": "GIZMO1", "color": "RED", "__typename": "Gizmo" },
				{ "id": "GIZMO2", "color": "GREEN", "__typename": "Gizmo" },
				{ "id": "GIZMO3", "color": null, "__typename": "Gizmo" }
			]
		}`

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				gizmos {
					... on Gizmo {
						id
						color
						__typename
					}
				}
			}
		`

		document := gqlparser.MustLoadQuery(schema, query)
		result := jsonToInterfaceMap(resultJSON)
		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.Equal(t, GraphqlErrors([]GraphqlError{{Message: "field failed to resolve", Path: ast.Path{ast.PathName("gizmos"), ast.PathIndex(2), ast.PathName("color")}, Extensions: nil}}), errs)
		require.Equal(t, jsonToInterfaceMap(`{ "gizmos": [ { "id": "GIZMO1", "color": "RED", "__typename": "Gizmo" }, { "id": "GIZMO2", "color": "GREEN", "__typename": "Gizmo" }, null ]	}`), result)
	})

	t.Run("inline fragment inside interface", func(t *testing.T) {
		ddl := `
		interface Critter {
			id: ID!
		}

		type Gizmo implements Critter {
			id: ID!
			color: String!
		}

		type Gremlin implements Critter {
			id: ID!
			name: String!
		}

		type Query {
			critters: [Critter]!
		}`

		resultJSON := `{
			"critters": [
				{ "id": "GIZMO1", "color": "RED", "__typename": "Gizmo" },
				{ "id": "GREMLIN1", "name": "Spikey", "__typename": "Gremlin" },
				{ "id": "GIZMO2", "color": null, "__typename": "Gizmo" }
			]
		}`

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				critters {
					id
					... on Gizmo {
						color
						__typename
					}
					... on Gremlin {
						name
						__typename
					}
				}
			}
		`

		document := gqlparser.MustLoadQuery(schema, query)
		result := jsonToInterfaceMap(resultJSON)
		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.Equal(t, GraphqlErrors([]GraphqlError{{Message: "field failed to resolve", Path: ast.Path{ast.PathName("critters"), ast.PathIndex(2), ast.PathName("color")}, Extensions: nil}}), errs)
		require.Equal(t, jsonToInterfaceMap(`{ "critters": [ { "id": "GIZMO1", "color": "RED", "__typename": "Gizmo"  }, { "id": "GREMLIN1", "name": "Spikey", "__typename": "Gremlin" }, null ]	}`), result)
	})

	t.Run("fragment spread inside interface", func(t *testing.T) {
		ddl := `
		interface Critter {
			id: ID!
		}

		type Gizmo implements Critter {
			id: ID!
			color: String!
		}

		type Gremlin implements Critter {
			id: ID!
			name: String!
		}

		type Query {
			critters: [Critter]!
		}`

		resultJSON := `{
			"critters": [
				{ "id": "GIZMO1", "color": "RED", "__typename": "Gizmo" },
				{ "id": "GREMLIN1", "name": "Spikey", "__typename": "Gremlin" },
				{ "id": "GIZMO2", "color": null, "__typename": "Gizmo" }
			]
		}`

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			fragment CritterDetails on Critter {
				... on Gizmo {
					color
					__typename
				}
				... on Gremlin {
					name
					__typename
				}
			}

			{
				critters {
					id
					... CritterDetails
				}
			}
		`

		document := gqlparser.MustLoadQuery(schema, query)
		result := jsonToInterfaceMap(resultJSON)
		errs, err := bubbleUpNullValuesInPlace(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.Equal(t, GraphqlErrors([]GraphqlError{{Message: "field failed to resolve", Path: ast.Path{ast.PathName("critters"), ast.PathIndex(2), ast.PathName("color")}, Extensions: nil}}), errs)
		require.Equal(t, jsonToInterfaceMap(`{ "critters": [ { "id": "GIZMO1", "color": "RED", "__typename": "Gizmo"  }, { "id": "GREMLIN1", "name": "Spikey", "__typename": "Gremlin" }, null ]	}`), result)
	})
}

func TestFormatResponseBody(t *testing.T) {
	t.Run("simple response with no errors", func(t *testing.T) {
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
		}`

		result := jsonToInterfaceMap(`
			{
				"gizmos": [
					{ "color": "RED","owner": { "name": "Owner1", "id": "1" }, "id": "GIZMO1" },
					{ "color": "BLUE","owner": { "name": "Owner2", "id": "2" }, "id": "GIZMO2" },
					{ "color": "GREEN","owner": { "name": "Owner3", "id": "3" }, "id": "GIZMO3" }
				]
			}
		`)

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				gizmos {
					id
					color
					owner {
						id
						name
					}
				}
			}`

		expectedJSON := `
			{
				"gizmos": [
					{ "id": "GIZMO1", "color": "RED", "owner": { "id": "1", "name": "Owner1" } },
					{ "id": "GIZMO2", "color": "BLUE", "owner": { "id": "2", "name": "Owner2" } },
					{ "id": "GIZMO3", "color": "GREEN", "owner": { "id": "3", "name": "Owner3" } }
				]
			}`

		document := gqlparser.MustLoadQuery(schema, query)
		bodyJSON, err := formatResponseBody(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.JSONEq(t, expectedJSON, bodyJSON)
	})

	t.Run("simple response with errors", func(t *testing.T) {
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
		}`

		result := jsonToInterfaceMap(`
			{
				"gizmos": [
					{ "color": "RED","owner": { "name": "Owner1", "id": "1" }, "id": "GIZMO1" },
					{ "color": "BLUE","owner": { "name": "Owner2", "id": "2" }, "id": "GIZMO2" },
					{ "color": "GREEN","owner": { "name": "Owner3", "id": "3" }, "id": "GIZMO3" }
				]
			}
		`)

		schema := gqlparser.MustLoadSchema(&ast.Source{Name: "fixture", Input: ddl})

		query := `
			{
				gizmos {
					id
					color
					owner {
						id
						name
					}
				}
			}`

		expectedJSON := `
			{
				"gizmos": [
					{ "id": "GIZMO1", "color": "RED", "owner": { "id": "1", "name": "Owner1" } },
					{ "id": "GIZMO2", "color": "BLUE", "owner": { "id": "2", "name": "Owner2" } },
					{ "id": "GIZMO3", "color": "GREEN", "owner": { "id": "3", "name": "Owner3" } }
				]
			}`

		document := gqlparser.MustLoadQuery(schema, query)
		bodyJSON, err := formatResponseBody(schema, document.Operations[0].SelectionSet, result)
		require.NoError(t, err)
		require.JSONEq(t, expectedJSON, bodyJSON)
	})
}

type queryExecution2Fixture struct {
	services  []testService
	variables map[string]interface{}
	query     string
	expected  string
	resp      *graphql.Response
	debug     *DebugInfo
	errors    gqlerror.List
}

func (f *queryExecution2Fixture) checkSuccess(t *testing.T) {
	f.run(t)

	assert.Empty(t, f.resp.Errors)
	jsonEqWithOrder(t, f.expected, string(f.resp.Data))
}

func (f *queryExecution2Fixture) run(t *testing.T) {
	var services []*Service
	var schemas []*ast.Schema

	for _, s := range f.services {
		serv := httptest.NewServer(s.handler)
		defer serv.Close()

		schema := gqlparser.MustLoadSchema(&ast.Source{Input: s.schema})
		services = append(services, &Service{
			ServiceURL: serv.URL,
			Schema:     schema,
		})

		schemas = append(schemas, schema)
	}

	merged, err := MergeSchemas(schemas...)
	require.NoError(t, err)

	es := newExecutableSchema(nil, 50, nil, services...)
	es.MergedSchema = merged
	es.BoundaryQueries = buildBoundaryQueriesMap(services...)
	es.Locations = buildFieldURLMap(services...)
	es.IsBoundary = buildIsBoundaryMap(services...)
	query := gqlparser.MustLoadQuery(merged, f.query)
	vars := f.variables
	if vars == nil {
		vars = map[string]interface{}{}
	}
	ctx := testContextWithVariables(vars, query.Operations[0])
	if f.debug != nil {
		ctx = context.WithValue(ctx, DebugKey, *f.debug)
	}
	f.resp = es.NewPipelineExecuteQuery(ctx)
	f.resp.Extensions = graphql.GetExtensions(ctx)

	if len(f.errors) == 0 {
		assert.Empty(t, f.resp.Errors)
		jsonEqWithOrder(t, f.expected, string(f.resp.Data))
	} else {
		require.Equal(t, len(f.errors), len(f.resp.Errors))
		for i := range f.errors {
			delete(f.resp.Errors[i].Extensions, "serviceUrl")
			assert.Equal(t, *f.errors[i], *f.resp.Errors[i])
		}
	}
}

func jsonToInterfaceMap(jsonString string) map[string]interface{} {
	var outputMap map[string]interface{}
	err := json.Unmarshal([]byte(jsonString), &outputMap)
	if err != nil {
		panic(err)
	}

	return outputMap
}

func jsonToInterfaceSlice(jsonString string) []interface{} {
	var outputSlice []interface{}
	err := json.Unmarshal([]byte(jsonString), &outputSlice)
	if err != nil {
		panic(err)
	}

	return outputSlice
}
