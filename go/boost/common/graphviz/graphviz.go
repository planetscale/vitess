package graphviz

import (
	"fmt"
	"html"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

const DefaultFontName = "Arial"

type Node struct {
	Subgraph  string
	Attr      map[string]string
	TableAttr map[string]string
	table     [][]any
}

func JSON(x proto.Message) string {
	m := &jsonpb.Marshaler{EmitDefaults: true, Indent: "    "}
	j, err := m.MarshalToString(x)
	if err != nil {
		panic(err)
	}
	return j
}

type Escaped string

type Cell struct {
	Txt  Escaped
	Attr map[string]string
}

func Fmt(pat string, args ...any) Escaped {
	for i, a := range args {
		if str, ok := a.(string); ok {
			args[i] = html.EscapeString(str)
		}
	}
	return Escaped(fmt.Sprintf(pat, args...))
}

func (n *Node) Row(col ...any) {
	n.table = append(n.table, col)
}

func (n *Node) renderTable(w io.Writer) {
	var maxcol int
	for _, r := range n.table {
		if len(r) > maxcol {
			maxcol = len(r)
		}
	}

	fmt.Fprintf(w, "<TABLE")
	for k, v := range n.TableAttr {
		fmt.Fprintf(w, " %s=%q", k, v)
	}
	fmt.Fprintf(w, ">\n")
	for _, r := range n.table {
		fmt.Fprintf(w, "<TR>")
		for idx, col := range r {
			fmt.Fprintf(w, "<TD")
			if maxcol > 1 && idx == len(r)-1 && idx < maxcol {
				fmt.Fprintf(w, " COLSPAN=\"%d\"", maxcol-idx)
			}
			switch txt := col.(type) {
			case Cell:
				for k, v := range txt.Attr {
					fmt.Fprintf(w, " %s=%q", k, v)
				}
				fmt.Fprintf(w, ">%s</TD>", txt.Txt)
			case Escaped:
				fmt.Fprintf(w, ">%s</TD>", txt)
			case string:
				fmt.Fprintf(w, ">%s</TD>", html.EscapeString(txt))
			default:
				panic("unexpected value in row")
			}
		}
		fmt.Fprintf(w, "</TR>\n")
	}
	fmt.Fprintf(w, "</TABLE>")
}

type Edge[I comparable] struct {
	from, to I
	Attr     map[string]string
}

type Graph[I comparable] struct {
	Clustering bool
	Attr       map[string]string

	nodes map[I]*Node
	edges []*Edge[I]
}

func NewGraph[I comparable]() *Graph[I] {
	return &Graph[I]{nodes: map[I]*Node{}, Attr: map[string]string{}}
}

func (g *Graph[I]) Node(idx I) (*Node, bool) {
	n, ok := g.nodes[idx]
	return n, ok
}

func (g *Graph[I]) AddNode(idx I) *Node {
	n := &Node{Attr: map[string]string{}, TableAttr: map[string]string{}}
	g.nodes[idx] = n
	return n
}

func (g *Graph[I]) AddEdge(from, to I) *Edge[I] {
	e := &Edge[I]{from: from, to: to, Attr: map[string]string{}}
	g.edges = append(g.edges, e)
	return e
}

func (g *Graph[I]) renderSubgraph(w io.Writer, name string, nodes map[I]*Node) {
	var indent = "\t"
	if name != "" {
		fmt.Fprintf(w, "\tsubgraph cluster_%s {\n", name)
		indent = "\t\t"
	}

	for idx, node := range nodes {
		fmt.Fprintf(w, "%sn%v [", indent, idx)
		for k, v := range node.Attr {
			fmt.Fprintf(w, "%s=%q ", k, v)
		}
		if node.table != nil {
			fmt.Fprintf(w, "label=<\n")
			node.renderTable(w)
			fmt.Fprintf(w, ">")
		}
		fmt.Fprintf(w, "\n\t]\n")
	}

	if name != "" {
		fmt.Fprintf(w, "\t}\n")
	}
}

func (g *Graph[I]) Render(w io.Writer) {
	fmt.Fprintf(w, "digraph {\n")
	for k, v := range g.Attr {
		fmt.Fprintf(w, "\t%s=%q\n", k, v)
	}
	fmt.Fprintf(w, "\tnode [shape=plain, fontsize=12, fontname=%q]\n", DefaultFontName)

	if g.Clustering {
		var subgraphs = make(map[string]map[I]*Node)
		for idx, n := range g.nodes {
			sg, ok := subgraphs[n.Subgraph]
			if !ok {
				sg = map[I]*Node{}
				subgraphs[n.Subgraph] = sg
			}
			sg[idx] = n
		}

		for name, subgraph := range subgraphs {
			g.renderSubgraph(w, name, subgraph)
		}
	} else {
		g.renderSubgraph(w, "", g.nodes)
	}

	for _, e := range g.edges {
		fmt.Fprintf(w, "\tn%v -> n%v", e.from, e.to)
		if len(e.Attr) > 0 {
			fmt.Fprintf(w, " [")
			for k, v := range e.Attr {
				fmt.Fprintf(w, "%s=%q ", k, v)
			}
			fmt.Fprintf(w, "]")
		}
		fmt.Fprintf(w, "\n")
	}

	fmt.Fprintf(w, "}\n")
}

func (g *Graph[I]) View(t testing.TB) {
	var dot strings.Builder
	g.Render(&dot)
	RenderGraphviz(t, dot.String())
}

func RenderGraphviz(t testing.TB, dot string) {
	const htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>GraphViz Viewer</title>
	<script src="https://cdn.jsdelivr.net/npm/svg-pan-zoom/dist/svg-pan-zoom.min.js"></script>
	<style>
		#graph svg {
			width: 90vw;
			height: 90vh;
			border-width: 1px;
			border-style: dotted;
		}
	</style>
</head>
<body>
    <div id="graph"></div>
	<script type="module">
		import { Graphviz } from "https://cdn.jsdelivr.net/npm/@hpcc-js/wasm/dist/graphviz.js";

		const graphviz = await Graphviz.load();
		const dot = %q;
		const svg = graphviz.dot(dot);
		const div = document.getElementById("graph");
		div.innerHTML = graphviz.layout(dot, "svg", "dot");
		svgPanZoom(div.querySelector('svg'), { controlIconsEnabled: true });
	</script>
</body>
</html>
`

	tmpfile, err := os.CreateTemp("", "boost_graph_*.html")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fmt.Fprintf(tmpfile, htmlTemplate, dot)
	if err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	browsers := func() []string {
		var cmds []string
		if userBrowser := os.Getenv("BROWSER"); userBrowser != "" {
			cmds = append(cmds, userBrowser)
		}
		switch runtime.GOOS {
		case "darwin":
			cmds = append(cmds, "/usr/bin/open")
		case "windows":
			cmds = append(cmds, "cmd /c start")
		default:
			if os.Getenv("DISPLAY") != "" {
				// xdg-open is only for use in a desktop environment.
				cmds = append(cmds, "xdg-open")
			}
			cmds = append(cmds, "chrome", "google-chrome", "chromium", "firefox", "sensible-browser")
		}
		return cmds
	}

	for _, b := range browsers() {
		args := strings.Split(b, " ")
		if len(args) == 0 {
			continue
		}
		viewer := exec.Command(args[0], append(args[1:], tmpfile.Name())...)
		viewer.Stderr = os.Stderr
		if err := viewer.Start(); err == nil {
			return
		}
	}

	t.Errorf("failed to open browser for SVG debugging")
}