/*
Copyright 2021 The Vitess Authors.

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

package asthelpergen

import (
	"fmt"
	"go/types"

	"github.com/dave/jennifer/jen"
)

const (
	rewriteName = "rewrite"
	abort       = "errAbort"
)

type rewriteGen struct {
	ifaceName string
}

var _ generator2 = (*rewriteGen)(nil)

func (e rewriteGen) interfaceMethod(t types.Type, iface *types.Interface, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}
	/*
		func VisitAST(in AST) (bool, error) {
			if in == nil {
				return false, nil
			}
			switch a := inA.(type) {
			case *SubImpl:
				return VisitSubImpl(a, b)
			default:
				return false, nil
			}
		}
	*/
	stmts := []jen.Code{
		jen.If(jen.Id("node == nil").Block(returnNil())),
	}

	var cases []jen.Code
	_ = spi.findImplementations(iface, func(t types.Type) error {
		if _, ok := t.Underlying().(*types.Interface); ok {
			return nil
		}
		typeString := types.TypeString(t, noQualifier)
		funcName := rewriteName + printableTypeName(t)
		spi.addType(t)
		caseBlock := jen.Case(jen.Id(typeString)).Block(
			jen.Return(jen.Id(funcName).Call(jen.Id("parent, node, replacer, pre, post"))),
		)
		cases = append(cases, caseBlock)
		return nil
	})

	cases = append(cases,
		jen.Default().Block(
			jen.Comment("this should never happen"),
			returnNil(),
		))

	stmts = append(stmts, jen.Switch(jen.Id("node := node.(type)").Block(
		cases...,
	)))

	e.rewriteFunc(t, stmts, spi)
	return nil
}

func (e rewriteGen) structMethod(t types.Type, strct *types.Struct, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	stmts := []jen.Code{
		jen.Var().Id("err").Error(),
		createCursor(),
		jen.If(jen.Id("!pre(&cur)")).Block(returnNil()),
	}
	stmts = append(stmts, e.rewriteAllStructFields(t, strct, spi, true)...)
	stmts = append(stmts,
		jen.If(jen.Id("err != nil")).Block(jen.Return(jen.Err())),
		jen.If(jen.Id("!post").Call(jen.Id("&cur"))).Block(jen.Return(jen.Id(abort))),
		returnNil(),
	)
	e.rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) ptrToStructMethod(t types.Type, strct *types.Struct, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	stmts := []jen.Code{
		/*
			if node == nil { return nil }
		*/
		jen.If(jen.Id("node == nil").Block(returnNil())),

		/*
			cur := Cursor{
				parent:   parent,
				replacer: replacer,
				node:     node,
			}
		*/
		createCursor(),

		/*
			if !pre(&cur) {
				return nil
			}
		*/
		jen.If(jen.Id("!pre(&cur)")).Block(returnNil()),
	}

	stmts = append(stmts, e.rewriteAllStructFields(t, strct, spi, false)...)

	stmts = append(stmts,
		jen.If(jen.Id("!post").Call(jen.Id("&cur"))).Block(jen.Return(jen.Id(abort))),
		returnNil(),
	)
	e.rewriteFunc(t, stmts, spi)

	return nil
}

func createCursor() *jen.Statement {
	return jen.Id("cur := Cursor").Values(
		jen.Dict{
			jen.Id("parent"):   jen.Id("parent"),
			jen.Id("replacer"): jen.Id("replacer"),
			jen.Id("node"):     jen.Id("node"),
		})
}

func (e rewriteGen) ptrToBasicMethod(t types.Type, _ *types.Basic, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
	 */

	stmts := []jen.Code{
		jen.Comment("ptrToBasicMethod"),
	}
	e.rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) sliceMethod(t types.Type, slice *types.Slice, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
		if node == nil {
				return nil
			}
			cur := Cursor{
				node:     node,
				parent:   parent,
				replacer: replacer,
			}
			if !pre(&cur) {
				return nil
			}
	*/
	stmts := []jen.Code{
		jen.If(jen.Id("node == nil").Block(returnNil())),
		createCursor(),
		jen.If(jen.Id("!pre(&cur)")).Block(returnNil()),
	}

	if shouldAdd(slice.Elem(), spi.iface()) {
		/*
			for i, el := range node {
						if err := rewriteRefOfLeaf(node, el, func(newNode, parent AST) {
							parent.(LeafSlice)[i] = newNode.(*Leaf)
						}, pre, post); err != nil {
							return err
						}
					}
		*/
		stmts = append(stmts,
			jen.For(jen.Id("i, el").Op(":=").Id("range node")).
				Block(e.rewriteChild(t, slice.Elem(), "notUsed", jen.Id("el"), jen.Index(jen.Id("i")), false)))
	}

	stmts = append(stmts,
		/*
			if !post(&cur) {
				return errAbort
			}
			return nil

		*/
		jen.If(jen.Id("!post").Call(jen.Id("&cur"))).Block(jen.Return(jen.Id(abort))),
		returnNil(),
	)
	e.rewriteFunc(t, stmts, spi)
	return nil
}

func (e rewriteGen) basicMethod(t types.Type, _ *types.Basic, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	stmts := []jen.Code{
		createCursor(),
		jen.If(jen.Id("!pre(&cur)")).Block(returnNil()),
		jen.If(jen.Id("!post").Call(jen.Id("&cur"))).Block(jen.Return(jen.Id(abort))),
		returnNil(),
	}

	e.rewriteFunc(t, stmts, spi)
	return nil
}

func (e rewriteGen) visitNoChildren(t types.Type, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
	 */

	stmts := []jen.Code{
		jen.Comment("ptrToStructMethod"),
	}
	e.rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) rewriteFunc(t types.Type, stmts []jen.Code, spi generatorSPI) {

	/*
		func (a *application) rewriteNodeType(parent AST, node NodeType, replacer replacerFunc) {
	*/

	typeString := types.TypeString(t, noQualifier)
	funcName := fmt.Sprintf("%s%s", rewriteName, printableTypeName(t))
	code := jen.Func().Id(funcName).Params(
		jen.Id(fmt.Sprintf("parent %s, node %s, replacer replacerFunc, pre, post ApplyFunc", e.ifaceName, typeString)),
	).Error().
		Block(stmts...)

	spi.addFunc(funcName, rewrite, code)
}

func (e rewriteGen) rewriteAllStructFields(t types.Type, strct *types.Struct, spi generatorSPI, fail bool) []jen.Code {
	/*
		if errF := rewriteAST(node, node.ASTType, func(newNode, parent AST) {
			err = vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] tried to replace '%s' on '%s'")
		}, pre, post); errF != nil {
			return errF
		}

	*/
	var output []jen.Code
	for i := 0; i < strct.NumFields(); i++ {
		field := strct.Field(i)
		if types.Implements(field.Type(), spi.iface()) {
			spi.addType(field.Type())
			output = append(output, e.rewriteChild(t, field.Type(), field.Name(), jen.Id("node").Dot(field.Name()), jen.Dot(field.Name()), fail))
			continue
		}
		slice, isSlice := field.Type().(*types.Slice)
		if isSlice && types.Implements(slice.Elem(), spi.iface()) {
			spi.addType(slice.Elem())
			id := jen.Id("i")
			if fail {
				id = jen.Id("_")
			}
			output = append(output,
				jen.For(jen.List(id, jen.Id("el")).Op(":=").Id("range node."+field.Name())).
					Block(e.rewriteChild(t, slice.Elem(), field.Name(), jen.Id("el"), jen.Dot(field.Name()).Index(id), fail)))
		}
	}
	return output
}

func failReplacer(t types.Type, f string) *jen.Statement {
	typeString := types.TypeString(t, noQualifier)
	return jen.Err().Op("=").Qual("vitess.io/vitess/go/vt/vterrors", "New").Call(
		jen.Qual("vitess.io/vitess/go/vt/proto/vtrpc", "Code_INTERNAL"),
		jen.Lit(fmt.Sprintf("[BUG] tried to replace '%s' on '%s'", f, typeString)),
	)
}

func (e rewriteGen) rewriteChild(t, field types.Type, fieldName string, param jen.Code, replace jen.Code, fail bool) jen.Code {
	/*
		if errF := rewriteAST(node, node.ASTType, func(newNode, parent AST) {
			parent.(*RefContainer).ASTType = newNode.(AST)
		}, pre, post); errF != nil {
			return errF
		}

		if errF := rewriteAST(node, el, func(newNode, parent AST) {
			parent.(*RefSliceContainer).ASTElements[i] = newNode.(AST)
		}, pre, post); errF != nil {
			return errF
		}

	*/
	funcName := rewriteName + printableTypeName(field)
	var replaceOrFail *jen.Statement
	if fail {
		replaceOrFail = failReplacer(t, fieldName)
	} else {
		replaceOrFail = jen.Id("parent").
			Assert(jen.Id(types.TypeString(t, noQualifier))).
			Add(replace).
			Op("=").
			Id("newNode").Assert(jen.Id(types.TypeString(field, noQualifier)))

	}
	funcBlock := jen.Func().Call(jen.Id("newNode, parent").Id(e.ifaceName)).
		Block(replaceOrFail)

	rewriteField := jen.If(
		jen.Id("errF := ").Id(funcName).Call(
			jen.Id("node"),
			param,
			funcBlock,
			jen.Id("pre"),
			jen.Id("post")),
		jen.Id("errF != nil").Block(jen.Return(jen.Id("errF"))))

	return rewriteField
}

var noQualifier = func(p *types.Package) string {
	return ""
}
