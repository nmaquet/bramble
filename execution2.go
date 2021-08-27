package bramble

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

// FIXME: dedupe result?
func extractBoundaryIDs(data interface{}, insertionPoint []string) ([]string, error) {
	ptr := data
	if len(insertionPoint) == 0 {
		switch ptr := ptr.(type) {
		case map[string]interface{}:
			value, ok := ptr["_id"]
			if !ok {
				return nil, errors.New("extractBoundaryIDs: unexpected missing '_id' in map")
			}
			str, ok := value.(string)
			if !ok {
				return nil, errors.New("extractBoundaryIDs: unexpected non string '_id' in map")
			}
			return []string{str}, nil
		case []interface{}:
			result := []string{}
			for innerPtr := range ptr {
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
		return []string{fmt.Sprintf(`{ %s(ids: %s) %s }`, parentTypeBoundaryField.Query, idsQL, selectionSetQL)}, nil
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

type ExecutionResult struct {
	ServiceURL     string
	InsertionPoint []string
	Data           map[string]interface{}
}

func mergeExecutionResults(results []ExecutionResult) (map[string]interface{}, error) {
	if len(results) == 0 {
		return nil, errors.New("mergeExecutionResults: nothing to merge")
	}
	if len(results) == 1 {
		return results[0].Data, nil
	}
	data := results[0].Data
	for _, result := range results[1:] {
		if err := shallowCopyIntoMap(result.Data, data, result.InsertionPoint); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func shallowCopyIntoMap(src map[string]interface{}, dst interface{}, insertionPoint []string) error {
	if len(insertionPoint) == 0 {
		switch ptr := dst.(type) {
		case map[string]interface{}:
			for k, v := range src {
				ptr[k] = v
			}
		default:
			return fmt.Errorf("shallowCopyIntoMap: unxpected type '%T' for top-level merge", ptr)
		}
	} else if len(insertionPoint) == 1 {
		switch ptr := dst.(type) {
		case map[string]interface{}:
			ptr = ptr[insertionPoint[0]].(map[string]interface{})
			if len(ptr) != 1 {
				return fmt.Errorf("shallowCopyIntoMap: expected src to have one key, not %d", len(ptr))
			}
			data, ok := src["_0"].(map[string]interface{})
			if !ok {
				return errors.New("shallowCopyIntoMap: expected src[\"_0\"] to be map[string]interface{}")
			}
			for k, v := range data {
				ptr[k] = v
			}
		case []interface{}:
			for _, dstValue := range ptr {
				// FIXME, remove these heinous casts
				dstID := dstValue.(map[string]interface{})[insertionPoint[0]].(map[string]interface{})["_id"]
				for _, srcValue := range src {
					srcID := srcValue.(map[string]interface{})["_id"]
					if srcID == dstID {
						for k, v := range srcValue.(map[string]interface{}) {
							// FIXME, remove these heinous casts
							dstValue.(map[string]interface{})[insertionPoint[0]].(map[string]interface{})[k] = v
						}
					}
				}
			}
		default:
			return fmt.Errorf("shallowCopyIntoMap: unxpected type '%T' for non top-level merge", ptr)
		}
	} else {
		switch ptr := dst.(type) {
		case map[string]interface{}:
			if err := shallowCopyIntoMap(src, ptr[insertionPoint[0]], insertionPoint[1:]); err != nil {
				return err
			}
		case []interface{}:
			for _, innerPtr := range ptr {
				if err := shallowCopyIntoMap(src, innerPtr, insertionPoint); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("shallowCopyIntoMap: unxpected type '%T' for non top-level merge", ptr)
		}
	}
	return nil
}
