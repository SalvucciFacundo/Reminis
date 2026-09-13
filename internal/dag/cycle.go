package dag

import (
	"errors"
	"fmt"
	"sort"
)

var (
	// ErrCycleDetected is returned when circular dependencies are detected in the task graph.
	ErrCycleDetected = errors.New("cycle detected in task graph")
)

// ValidateAcyclic checks that the given task graph is a Directed Acyclic Graph (DAG)
// using Kahn's algorithm. It returns a topologically sorted list of task IDs on success,
// or ErrCycleDetected if a circular dependency is detected.
func ValidateAcyclic(tasks []Task) ([]string, error) {
	if len(tasks) == 0 {
		return []string{}, nil
	}

	taskMap := make(map[string]Task, len(tasks))
	for _, t := range tasks {
		if _, exists := taskMap[t.ID]; exists {
			return nil, fmt.Errorf("duplicate task ID: %q", t.ID)
		}
		taskMap[t.ID] = t
	}

	// Validate dependencies exist
	for _, t := range tasks {
		for _, dep := range t.DependsOn {
			if _, exists := taskMap[dep]; !exists {
				return nil, fmt.Errorf("task %q depends on non-existent task %q", t.ID, dep)
			}
		}
	}

	inDegree := make(map[string]int, len(tasks))
	adj := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		inDegree[t.ID] = len(t.DependsOn)
		for _, dep := range t.DependsOn {
			adj[dep] = append(adj[dep], t.ID)
		}
	}

	var queue []string
	for _, t := range tasks {
		if inDegree[t.ID] == 0 {
			queue = append(queue, t.ID)
		}
	}
	sort.Strings(queue)

	var order []string
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		order = append(order, u)

		// For deterministic order, sort neighbors
		neighbors := adj[u]
		for _, v := range neighbors {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
				sort.Strings(queue)
			}
		}
	}

	if len(order) < len(tasks) {
		return nil, ErrCycleDetected
	}

	return order, nil
}
