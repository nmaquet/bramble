package bramble

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var (
	errNilBoundaryData   = errors.New("found a null when attempting to extract boundary ids")
	errNullBubbledToRoot = errors.New("bubbleUpNullValuesInPlace: null bubbled up to root")
)

type ExecutionResult struct {
	ServiceURL     string
	InsertionPoint []string
	Data           interface{}
}

type QueryExecution2 struct {
	Schema       *ast.Schema
	Errors       []*gqlerror.Error
	RequestCount int64

	// FIXME: implement
	maxRequest int64
	// FIXME: implement?
	tracer        opentracing.Tracer
	graphqlClient GraphQLClientInterface
	// FIXME: rename the entire type
	boundaryQueries BoundaryQueriesMap
}

func newQueryExecution2(client GraphQLClientInterface, schema *ast.Schema, boundaryQueries BoundaryQueriesMap) *QueryExecution2 {
	return &QueryExecution2{
		Schema:          schema,
		graphqlClient:   client,
		boundaryQueries: boundaryQueries,
	}
}

func (q *QueryExecution2) Execute(ctx context.Context, queryPlan QueryPlan) ([]ExecutionResult, []*gqlerror.List) {
	stepWg := &sync.WaitGroup{}
	readWg := &sync.WaitGroup{}
	resultsChan := make(chan ExecutionResult)
	results := []ExecutionResult{}
	for _, step := range queryPlan.RootSteps {
		if step.ServiceURL == internalServiceName {
			r, err := ExecuteBrambleStep(step)
			if err != nil {
				return nil, []*gqlerror.List{}
			}
			results = append(results, *r)
		}
		stepWg.Add(1)
		go q.ExecuteRootStep(ctx, *step, resultsChan, stepWg)

	}

	readWg.Add(1)
	go func() {
		for result := range resultsChan {
			results = append(results, result)
		}
		readWg.Done()
	}()
	stepWg.Wait()
	close(resultsChan)
	readWg.Wait()
	return results, nil
}

func (q *QueryExecution2) ExecuteRootStep(ctx context.Context, step QueryPlanStep, resultsChan chan ExecutionResult, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	var document string
	if step.ParentType == "Query" {
		document = "query " + formatSelectionSet(ctx, q.Schema, step.SelectionSet)
	} else if step.ParentType == "Mutation" {
		document = "mutation " + formatSelectionSet(ctx, q.Schema, step.SelectionSet)
	} else {
		// FIXME: error handling with channels
		panic("non mutation or query root step")
	}

	var data map[string]interface{}

	// FIXME: handle downstream errors in result object
	err := q.executeDocument(ctx, document, step.ServiceURL, &data)
	if err != nil {
		// FIXME: error handling with channels
		panic(err)
	}

	resultsChan <- ExecutionResult{step.ServiceURL, step.InsertionPoint, data}

	for _, childStep := range step.Then {
		boundaryIDs, err := extractBoundaryIDs(data, childStep.InsertionPoint)
		if err == errNilBoundaryData {
			continue
		}
		if err != nil {
			// FIXME: error handling with channels
			panic(err)
		}
		waitGroup.Add(1)
		go q.executeChildStep(ctx, *childStep, boundaryIDs, resultsChan, waitGroup)
	}
}

func (q *QueryExecution2) executeChildStep(ctx context.Context, step QueryPlanStep, boundaryIDs []string, resultsChan chan ExecutionResult, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	boundaryFieldGetter := q.boundaryQueries.Query(step.ServiceURL, step.ParentType)

	documents, err := buildBoundaryQueryDocuments(ctx, q.Schema, step, boundaryIDs, boundaryFieldGetter, 50)
	if err != nil {
		// FIXME: error handling with channels
		panic(err)
	}

	data, err := q.executeBoundaryQuery(ctx, documents, step.ServiceURL, boundaryFieldGetter)
	if err != nil {
		// FIXME: error handling with channels
		panic(err)
	}

	resultsChan <- ExecutionResult{step.ServiceURL, step.InsertionPoint, data}

	for _, childStep := range step.Then {
		boundaryIDs, err := extractBoundaryIDs(data, childStep.InsertionPoint[1:]) // FIXME: validate this always holds true
		if err == errNilBoundaryData {
			continue
		}
		if err != nil {
			// FIXME: error handling with channels
			panic(err)
		}
		waitGroup.Add(1)
		go q.executeChildStep(ctx, *childStep, boundaryIDs, resultsChan, waitGroup)
	}
}

func (q *QueryExecution2) executeBoundaryQuery(ctx context.Context, documents []string, serviceURL string, boundaryFieldGetter BoundaryQuery) ([]interface{}, error) {
	output := make([]interface{}, 0, 0)
	if !boundaryFieldGetter.Array {
		for _, document := range documents {
			partialData := make(map[string]interface{})
			err := q.executeDocument(ctx, document, serviceURL, &partialData)
			if err != nil {
				return nil, err
			}
			for _, value := range partialData {
				output = append(output, value)
			}
		}
		return output, nil
	}

	if len(documents) != 1 {
		return nil, errors.New("there should only be a single document for array boundary field lookups")
	}

	data := struct {
		Result []interface{} `json:"_result"`
	}{}

	err := q.executeDocument(ctx, documents[0], serviceURL, &data)
	return data.Result, err
}

func (q *QueryExecution2) executeDocument(ctx context.Context, document string, serviceURL string, response interface{}) error {
	req := NewRequest(document)
	req.Headers = GetOutgoingRequestHeadersFromContext(ctx)
	err := q.graphqlClient.Request(ctx, serviceURL, req, &response)
	return err
}

func ExecuteBrambleStep(queryPlanStep *QueryPlanStep) (*ExecutionResult, error) {
	result, err := BuildTypenameResponseMap(queryPlanStep.SelectionSet, queryPlanStep.ParentType)
	if err != nil {
		return nil, err
	}
	return &ExecutionResult{
		ServiceURL:     internalServiceName,
		InsertionPoint: []string{},
		Data:           result,
	}, nil
}

func BuildTypenameResponseMap(selectionSet ast.SelectionSet, parentTypeName string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, field := range selectionSetToFields(selectionSet) {
		if field.SelectionSet != nil {
			if field.Definition.Type.NamedType == "" {
				return nil, fmt.Errorf("expected named type")
			}

			if !hasNamespaceDirective(field.Directives) {
				return nil, fmt.Errorf("expected namespace directive")
			}

			var err error
			result[field.Alias], err = BuildTypenameResponseMap(field.SelectionSet, field.Definition.Type.Name())
			if err != nil {
				return nil, err
			}
		} else {
			if field.Name != "__typename" {
				return nil, fmt.Errorf("expected __typename")
			}
			result[field.Alias] = parentTypeName
		}
	}
	return result, nil
}

func hasNamespaceDirective(directiveList ast.DirectiveList) bool {
	for _, directive := range directiveList {
		if directive.Name == "@namespace" {
			return true
		}
	}
	return false
}

// FIXME: dedupe result?
func extractBoundaryIDs(data interface{}, insertionPoint []string) ([]string, error) {
	ptr := data
	if ptr == nil {
		return nil, errNilBoundaryData
	}
	if len(insertionPoint) == 0 {
		switch ptr := ptr.(type) {
		case map[string]interface{}:
			var id string
			var ok bool
			id, ok = ptr["_id"].(string)
			if !ok {
				id, ok = ptr["id"].(string)
			}
			if !ok {
				return nil, errors.New("extractBoundaryIDs: unexpected missing '_id' or 'id' in map")
			}
			return []string{id}, nil
		case []interface{}:
			result := []string{}
			for _, innerPtr := range ptr {
				ids, err := extractBoundaryIDs(innerPtr, insertionPoint)
				if err != nil {
					return nil, err
				}
				result = append(result, ids...)
			}
			return result, nil
		default:
			return nil, fmt.Errorf("extractBoundaryIDs: unexpected type: %T", ptr)
		}
	}
	switch ptr := ptr.(type) {
	case map[string]interface{}:
		if len(insertionPoint) == 1 {
			return extractBoundaryIDs(ptr[insertionPoint[0]], nil)
		} else {
			return extractBoundaryIDs(ptr[insertionPoint[0]], insertionPoint[1:])
		}
	case []interface{}:
		result := []string{}
		for _, innerPtr := range ptr {
			ids, err := extractBoundaryIDs(innerPtr, insertionPoint)
			if err != nil {
				return nil, err
			}
			result = append(result, ids...)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("extractBoundaryIDs: unexpected type: %T", ptr)
	}
}

func buildBoundaryQueryDocuments(ctx context.Context, schema *ast.Schema, step QueryPlanStep, ids []string, parentTypeBoundaryField BoundaryQuery, batchSize int) ([]string, error) {
	selectionSetQL := formatSelectionSetSingleLine(ctx, schema, step.SelectionSet)
	if parentTypeBoundaryField.Array {
		qids := []string{}
		for _, id := range ids {
			qids = append(qids, fmt.Sprintf("%q", id))
		}
		idsQL := fmt.Sprintf("[%s]", strings.Join(qids, ", "))
		return []string{fmt.Sprintf(`{ _result: %s(ids: %s) %s }`, parentTypeBoundaryField.Query, idsQL, selectionSetQL)}, nil
	}

	var (
		documents      []string
		selectionIndex int
	)
	for _, batch := range batchBy(ids, batchSize) {
		var selections []string
		for _, id := range batch {
			selection := fmt.Sprintf("%s: %s(id: %q) %s", nodeAlias(selectionIndex), parentTypeBoundaryField.Query, id, selectionSetQL)
			selections = append(selections, selection)
			selectionIndex++
		}
		document := "{ " + strings.Join(selections, " ") + " }"
		documents = append(documents, document)
	}

	return documents, nil
}

func batchBy(items []string, batchSize int) (batches [][]string) {
	for batchSize < len(items) {
		items, batches = items[batchSize:], append(batches, items[0:batchSize:batchSize])
	}

	return append(batches, items)
}

func mergeExecutionResults(results []ExecutionResult) (map[string]interface{}, error) {
	if len(results) == 0 {
		return nil, errors.New("mergeExecutionResults: nothing to merge")
	}

	if len(results) == 1 {
		dataMap, ok := results[0].Data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("a complete graphql response should be map[string]interface{}, got %T", results[0].Data)
		}
		return dataMap, nil
	}

	data := results[0].Data
	for _, result := range results[1:] {
		if err := mergeExecutionResultsRec(result.Data, data, result.InsertionPoint); err != nil {
			return nil, err
		}
	}

	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("merged execution results should be map[string]interface{}, got %T", data)
	}

	return dataMap, nil
}

func mergeExecutionResultsRec(src interface{}, dst interface{}, insertionPoint []string) error {
	// base case
	if len(insertionPoint) == 0 {
		switch ptr := dst.(type) {
		case map[string]interface{}:
			switch src := src.(type) {
			// base case for root step merging
			case map[string]interface{}:
				for k, v := range src {
					ptr[k] = v
				}

			// base case for children step merging
			case []interface{}:
				boundaryResults, err := getBoundaryFieldResults(src)
				if err != nil {
					return err
				}

				dstID, err := boundaryIDFromMap(ptr)
				if err != nil {
					return err
				}

				for _, result := range boundaryResults {
					srcID, err := boundaryIDFromMap(result)
					if err != nil {
						return err
					}
					if srcID == dstID {
						for k, v := range result {
							if k == "_id" || k == "id" {
								continue
							}

							ptr[k] = v
						}
					}
				}

			}
		default:
			return fmt.Errorf("mergeExecutionResultsRec: unxpected type '%T' for top-level merge", ptr)
		}
		return nil
	}

	// recursive case
	switch ptr := dst.(type) {
	case map[string]interface{}:
		switch ptr := ptr[insertionPoint[0]].(type) {
		case []interface{}:
			for _, innerPtr := range ptr {
				if err := mergeExecutionResultsRec(src, innerPtr, insertionPoint[1:]); err != nil {
					return err
				}
			}
		default:
			if err := mergeExecutionResultsRec(src, ptr, insertionPoint[1:]); err != nil {
				return err
			}
		}
	case []interface{}:
		for _, innerPtr := range ptr {
			if err := mergeExecutionResultsRec(src, innerPtr, insertionPoint[1:]); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("mergeExecutionResultsRec: unxpected type '%T' for non top-level merge", ptr)
	}
	return nil
}

func boundaryIDFromMap(val interface{}) (string, error) {
	boundaryMap, ok := val.(map[string]interface{})
	if !ok {
		return "", errors.New("boundaryIDFromMap: expected map to extract boundary id")
	}
	id, ok := boundaryMap["_id"].(string)
	if ok {
		return id, nil
	}
	id, ok = boundaryMap["id"].(string)
	if ok {
		return id, nil
	}
	return "", errors.New("boundaryIDFromMap: 'id' or '_id' not found")
}

func getBoundaryFieldResults(src interface{}) ([]map[string]interface{}, error) {
	slice, ok := src.([]interface{})
	if !ok {
		return nil, fmt.Errorf("getBoundaryFieldResults: expected value to be a '[]map[string]interface{}' but got '%T'", slice)
	}
	var result []map[string]interface{}
	for i, element := range slice {
		elementMap, ok := element.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("getBoundaryFieldResults: expect value at index %d to be map[string]interface{}' but got '%T'", i, element)
		}
		result = append(result, elementMap)
	}
	return result, nil
}

// bubbleUpNullValuesInPlace checks for expected null values (as per schema) and bubbles them up if needed, and checks for
// unexpected null values and returns errors for each (these unexpected nulls are also bubbled up).
// See https://spec.graphql.org/June2018/#sec-Errors-and-Non-Nullability
func bubbleUpNullValuesInPlace(schema *ast.Schema, selectionSet ast.SelectionSet, result map[string]interface{}) (GraphqlErrors, error) {
	errs, bubbleUp, err := bubbleUpNullValuesInPlaceRec(schema, nil, selectionSet, result, ast.Path{})
	if err != nil {
		return nil, err
	}
	if bubbleUp {
		return nil, errNullBubbledToRoot
	}
	return errs, nil
}

func bubbleUpNullValuesInPlaceRec(schema *ast.Schema, currentType *ast.Type, selectionSet ast.SelectionSet, result interface{}, path ast.Path) (errs GraphqlErrors, bubbleUp bool, err error) {
	switch result := result.(type) {
	case map[string]interface{}:
		for _, selection := range selectionSet {
			switch selection := selection.(type) {
			case *ast.Field:
				field := selection
				value := result[field.Alias]
				if value == nil {
					if field.Definition.Type.NonNull {
						errs = append(errs, GraphqlError{Message: "field failed to resolve", Path: append(path, ast.PathName(field.Alias)), Extensions: nil})
						bubbleUp = true
					}
					return
				}
				if field.SelectionSet != nil {
					lowerErrs, lowerBubbleUp, lowerErr := bubbleUpNullValuesInPlaceRec(schema, field.Definition.Type, field.SelectionSet, value, append(path, ast.PathName(field.Alias)))
					if lowerErr != nil {
						return nil, false, lowerErr
					}
					if lowerBubbleUp {
						if field.Definition.Type.NonNull {
							bubbleUp = true
						} else {
							result[field.Alias] = nil
						}
					}
					errs = append(errs, lowerErrs...)
				}
			case *ast.FragmentSpread:
				fragment := selection
				typename, ok := result["__typename"].(string)
				if !ok {
					return nil, false, errors.New("missing expected __typename")
				}
				if typename != fragment.Definition.TypeCondition && !implementsInterface(schema, typename, fragment.Definition.TypeCondition) {
					continue
				}
				lowerErrs, lowerBubbleUp, lowerErr := bubbleUpNullValuesInPlaceRec(schema, nil, fragment.Definition.SelectionSet, result, path)
				if lowerErr != nil {
					return nil, false, lowerErr
				}
				bubbleUp = lowerBubbleUp
				errs = append(errs, lowerErrs...)
			case *ast.InlineFragment:
				fragment := selection
				typename, ok := result["__typename"].(string)
				if !ok {
					return nil, false, errors.New("missing expected __typename")
				}
				if typename != fragment.TypeCondition && !implementsInterface(schema, typename, fragment.TypeCondition) {
					continue
				}
				lowerErrs, lowerBubbleUp, lowerErr := bubbleUpNullValuesInPlaceRec(schema, nil, fragment.SelectionSet, result, path)
				if lowerErr != nil {
					return nil, false, lowerErr
				}
				bubbleUp = lowerBubbleUp
				errs = append(errs, lowerErrs...)
			default:
				err = fmt.Errorf("unknown selection type: %T", selection)
				return
			}
		}
	case []interface{}:
		for i, value := range result {
			lowerErrs, lowerBubbleUp, lowerErr := bubbleUpNullValuesInPlaceRec(schema, currentType, selectionSet, value, append(path, ast.PathIndex(i)))
			if lowerErr != nil {
				return nil, false, lowerErr
			}
			if lowerBubbleUp {
				if currentType.Elem.NonNull {
					bubbleUp = true
				} else {
					result[i] = nil
				}
			}
			errs = append(errs, lowerErrs...)
		}
	default:
		return nil, false, fmt.Errorf("bubbleUpNullValuesInPlaceRec: unxpected result type '%T'", result)
	}
	return
}

func formatResponseBody(selectionSet ast.SelectionSet, result map[string]interface{}) (string, error) {
	return formatResponseDataRec(selectionSet, result)
}

func formatResponseDataRec(selectionSet ast.SelectionSet, result interface{}) (string, error) {
	var buf bytes.Buffer
	if result == nil {
		return "null", nil
	}
	switch result := result.(type) {
	case map[string]interface{}:
		buf.WriteString("{")
		fields := selectionFields(selectionSet)
		for i, field := range fields {
			fieldData, ok := result[field.Alias]
			if !ok {
				return "", fmt.Errorf("could not find value in data for field %s", field.Alias)
			}
			buf.WriteString(fmt.Sprintf(`"%s":`, field.Alias))
			if field.SelectionSet != nil {
				innerBody, err := formatResponseDataRec(field.SelectionSet, fieldData)
				if err != nil {
					return "", err
				}
				buf.WriteString(innerBody)
			} else {
				fieldJSON, err := json.Marshal(&fieldData)
				if err != nil {
					return "", err
				}
				buf.Write(fieldJSON)
			}

			if i < len(fields)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString("}")
	case []interface{}:
		buf.WriteString("[")
		for i, v := range result {
			innerBody, err := formatResponseDataRec(selectionSet, v)
			if err != nil {
				return "", err
			}
			buf.WriteString(innerBody)

			if i < len(result)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString("]")
	}
	return buf.String(), nil
}

func selectionFields(selectionSet ast.SelectionSet) []*ast.Field {
	var result []*ast.Field
	for _, selection := range selectionSet {
		switch selection := selection.(type) {
		case *ast.Field:
			result = append(result, selection)
		case *ast.FragmentSpread:
			definition := selection.Definition
			result = append(result, selectionFields(definition.SelectionSet)...)
		case *ast.InlineFragment:
			result = append(result, selectionFields(selection.SelectionSet)...)
		}
	}
	return result
}

func implementsInterface(schema *ast.Schema, objectType, interfaceType string) bool {
	for _, def := range schema.Implements[objectType] {
		if def.Name == interfaceType {
			return true
		}
	}
	return false
}
