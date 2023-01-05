/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

// RewriteCOW allows for Copy On Write rewriting of the AST
func RewriteCOW(
	node SQLNode,                       // the root of the node we will vist
	apply ApplyFunc,                    // the visitor function
	cloned func(before, after SQLNode), // any nodes that are copied because a child was replaced will be seen by this function
) SQLNode {
	c := &cowCursor{
		f:        apply,
		node:     node,
		parent:   nil,
		replaced: nil,
		cloned:   cloned,
	}

	return nil
}

type cowCursor struct {
	f        ApplyFunc
	node     SQLNode // the current node we are visiting
	parent   SQLNode // the parent of the current node
	replaced SQLNode // if the user did a Replace, we have the new node here
	cloned   func(old, new SQLNode)
}

func (c *cowCursor) Node() SQLNode {
	return c.node
}

func (c *cowCursor) Parent() SQLNode {
	return c.parent
}

func (c *cowCursor) Replace(newNode SQLNode) {
	c.replaced = newNode
}

func (c *cowCursor) ReplacerF() func(SQLNode) {
	panic("COW rewrite does not support this")
}

func (c *cowCursor) ReplaceAndRevisit(SQLNode) {
	panic("COW rewrite does not support this")
}

var _ Cursor = (*cowCursor)(nil)
