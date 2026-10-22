package config

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"gopkg.in/yaml.v3"
)

var (
	yamlBool  = regexp.MustCompile(`^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	yamlFloat = regexp.MustCompile(`^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$`)
	yamlInt   = regexp.MustCompile(`^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$`)
	yamlNull  = regexp.MustCompile(`^(?:~|null|Null|NULL|)$`)
	yamlMerge = regexp.MustCompile(`^(?:<<)$`)
)

func implicit(value string) string {
	if value == "" {
		return "null"
	}
	in := func(chars string) bool { return strings.IndexByte(chars, value[0]) >= 0 }
	switch {
	case in("yYnNtTfFoO") && yamlBool.MatchString(value):
		return "bool"
	case in("-+0123456789.") && yamlFloat.MatchString(value):
		return "float"
	case in("-+0123456789") && yamlInt.MatchString(value):
		return "int"
	case in("<") && yamlMerge.MatchString(value):
		return "merge"
	case in("~nN") && yamlNull.MatchString(value):
		return "null"
	}

	return ""
}

func sexagesimal(value string) []string { return strings.Split(value, ":") }

func constructInt(text string) (any, error) {
	value := strings.ReplaceAll(text, "_", "")
	sign := int64(1)
	if strings.HasPrefix(value, "-") {
		sign, value = -1, value[1:]
	} else if strings.HasPrefix(value, "+") {
		value = value[1:]
	}
	parse := func(digits string, base int) (any, error) {
		n, ok := new(big.Int).SetString(digits, base)
		if !ok {
			return nil, fmt.Errorf("invalid YAML int %q", text)
		}
		n.Mul(n, big.NewInt(sign))
		if n.IsInt64() {
			return int(n.Int64()), nil
		}
		return nil, fmt.Errorf("YAML int out of range %q", text)
	}
	switch {
	case value == "0":
		return 0, nil
	case strings.HasPrefix(value, "0b"):
		return parse(value[2:], 2)
	case strings.HasPrefix(value, "0x"):
		return parse(value[2:], 16)
	case strings.HasPrefix(value, "0"):
		return parse(value, 8)
	case strings.Contains(value, ":"):
		total := int64(0)
		for _, part := range sexagesimal(value) {
			digit, err := strconv.ParseInt(part, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid YAML int %q", text)
			}
			total = total*60 + digit
		}
		return int(sign * total), nil
	}
	return parse(value, 10)
}

func constructFloat(text string) (any, error) {
	value := strings.ToLower(strings.ReplaceAll(text, "_", ""))
	sign := 1.0
	if strings.HasPrefix(value, "-") {
		sign, value = -1, value[1:]
	} else if strings.HasPrefix(value, "+") {
		value = value[1:]
	}
	switch {
	case value == ".inf":
		return sign * math.Inf(1), nil
	case value == ".nan":
		return math.NaN(), nil
	case strings.Contains(value, ":"):
		total := 0.0
		for _, part := range sexagesimal(value) {
			digit, err := strconv.ParseFloat(part, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid YAML float %q", text)
			}
			total = total*60 + digit
		}
		return sign * total, nil
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML float %q", text)
	}
	return sign * f, nil
}

func constructBool(text string) (any, error) {
	switch strings.ToLower(text) {
	case "yes", "true", "on":
		return true, nil
	case "no", "false", "off":
		return false, nil
	}
	return nil, fmt.Errorf("invalid YAML bool %q", text)
}

func scalar(n *yaml.Node) (any, error) {
	if n.Style&(yaml.SingleQuotedStyle|yaml.DoubleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) != 0 && n.Style&yaml.TaggedStyle == 0 {
		return n.Value, nil
	}
	tag := ""
	if n.Style&yaml.TaggedStyle != 0 {
		switch n.ShortTag() {
		case "!!str":
			return n.Value, nil
		case "!!int":
			tag = "int"
		case "!!float":
			tag = "float"
		case "!!bool":
			tag = "bool"
		case "!!null":
			tag = "null"
		default:
			return nil, fmt.Errorf("unsupported YAML tag %s at line %d", n.Tag, n.Line)
		}
	} else {
		tag = implicit(n.Value)
	}
	switch tag {
	case "bool":
		return constructBool(n.Value)
	case "float":
		return constructFloat(n.Value)
	case "int":
		return constructInt(n.Value)
	case "null":
		return nil, nil
	}
	return n.Value, nil
}

func key(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if f, ok := v.(float64); ok {
		return pyval.Str(f)
	}
	return pyval.Str(v)
}

func pythonValue(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil, nil
		}
		return pythonValue(n.Content[0])
	case yaml.AliasNode:
		return pythonValue(n.Alias)
	case yaml.ScalarNode:
		return scalar(n)
	case yaml.SequenceNode:
		out := make([]any, 0, len(n.Content))
		for _, item := range n.Content {
			v, err := pythonValue(item)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case yaml.MappingNode:
		out := map[string]any{}
		merged := map[string]any{}
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if k.Kind == yaml.ScalarNode && k.Style == 0 && implicit(k.Value) == "merge" {

				sources := []*yaml.Node{v}
				if v.Kind == yaml.SequenceNode {
					sources = v.Content
				}
				for _, source := range sources {
					value, err := pythonValue(source)
					if err != nil {
						return nil, err
					}
					m, ok := value.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("YAML merge of a non-mapping at line %d", source.Line)
					}
					for mk, mv := range m {
						if _, seen := merged[mk]; !seen {
							merged[mk] = mv
						}
					}
				}
				continue
			}
			kv, err := pythonValue(k)
			if err != nil {
				return nil, err
			}
			vv, err := pythonValue(v)
			if err != nil {
				return nil, err
			}
			out[key(kv)] = vv
		}
		for mk, mv := range merged {
			if _, ok := out[mk]; !ok {
				out[mk] = mv
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported YAML node kind %d", n.Kind)
}

func SafeLoad(data []byte) (any, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	if root.Kind == 0 {
		return nil, nil
	}
	return pythonValue(&root)
}
