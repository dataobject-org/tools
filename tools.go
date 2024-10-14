package tools

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dataobject-org/object"
	"github.com/svcbase/base"
	"github.com/tidwall/gjson"
)

const (
	defaultBufSize = 4096
)

func InterfaceDefinition2SQL(i_type /*user/json*/, definition, identifier string) (sqlsql []string) {
	result := gjson.Parse(definition)
	if result.Exists() {
		result.ForEach(func(k, v gjson.Result) bool {
			name := k.String()
			i_definition := v.String()
			if v.Type.String() == "JSON" {
				asql := "insert into entity_" + i_type + "interface(time_created,time_updated,entity_id,name,definition) values("
				//identifier->code
				asql += base.SQL_now() + "," + base.SQL_now() + ",(select id from entity where code='" + identifier + "'),"
				asql += "'" + name + "',"
				switch base.DB_type {
				case base.SQLite:
					asql += base.SQLiteEscape(i_definition)
				case base.MySQL:
					asql += base.MySQLEscape(i_definition)
				}
				asql += ");"
				sqlsql = append(sqlsql, asql)
			}
			return true
		})
	}
	return
}

func Tablefile2SQL(identifier, definition, filename, comment, language_id string, writer *bufio.Writer) (nLines int, e error) {
	nLines = 0
	tablefile, ee := os.Open(filename)
	if ee != nil {
		e = errors.New("failed to open file: " + filename + " " + ee.Error())
	} else {
		o := gjson.Parse(definition)
		nn := 0
		separator := ""
		type propertyT struct {
			Name             string
			Type             string
			Languageadaptive bool //language_adaptive
			Icol             int  //column index
			Default          string
		}
		fieldnames := []string{}
		properties := []propertyT{}
		reader := bufio.NewReader(tablefile)
		titleline, err := reader.ReadString('\n')
		if err == nil {
			col_names := []string{}
			separator, col_names, err = base.Splitfield(titleline, "", "")
			nn = len(col_names)
			for i := 0; i < nn; i++ {
				name := base.TrimBLANK(col_names[i])
				name = strings.TrimLeft(name, string(base.BOM))
				if len(name) > 0 {
					p := o.Get(name)
					if p.Exists() {
						properties = append(properties, propertyT{name, p.Get("type").String(), p.Get("language_adaptive").Bool(), i, p.Get("default").String()})
						fieldnames = append(fieldnames, "`"+name+"`")
					} else {
						e = errors.New(identifier + "[" + name + "]not exists")
					}
				}
			}
		} else {
			e = errors.New("read title line err: " + err.Error())
		}
		if len(properties) > 0 {
			fields := strings.Join(fieldnames, ",") + ",time_created,time_updated"
			sysAcceptMultipleLanguage := base.SysAcceptMultipleLanguage()
			baseLanguageCode := base.Baselanguage_code()
			objectLanguage := o.Get("language").String()
			nn = len(properties)
			eof := false
			iid := 0
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					if err != io.EOF {
						e = errors.New("failed to finish reading the file: " + filename + " " + err.Error())
						break
					} //here reach the last line
					eof = true
				}
				ln := base.TrimBLANK(line)
				if len(ln) > 0 {
					_, cols, err := base.Splitfield(ln, "", separator)
					if err == nil {
						ncols := len(cols)
						cid := ""
						iid++
						asql := "INSERT INTO `" + identifier + "`(" + fields + ") VALUES("
						values := []string{}
						la_names := []string{}
						la_values := []string{}
						valueconsistent := 0 // multiple language valueconsistent
						val := ""
						for i := 0; i < nn; i++ { //properties
							icol := properties[i].Icol
							if icol >= ncols {
								val = properties[i].Default
							} else {
								val = base.TrimBLANK(cols[icol])
							}
							switch properties[i].Type {
							case "int":
								val = strconv.Itoa(base.Str2int(val))
								values = append(values, val)
								if properties[i].Name == "id" {
									cid = val
								} else if properties[i].Name == "valueconsistent" {
									valueconsistent = base.Str2int(val)
								}
							case "decimal", "float":
								if !base.IsNumber(val) {
									val = "0"
								}
								values = append(values, val)
							case "string", "text", "ipv4", "ipv6", "dotids":
								if properties[i].Languageadaptive {
									la_names = append(la_names, "`"+properties[i].Name+"`")
									la_values = append(la_values, val)
								}
								val = base.GetLanguageVersionText(val, baseLanguageCode, baseLanguageCode, "|")
								val = strings.ReplaceAll(val, "\\r", "\r")
								val = strings.ReplaceAll(val, "\\n", "\n")
								val = strings.ReplaceAll(val, "\\t", "\t")
								values = append(values, base.SQL_escape(val))
							case "date":
								val = strings.Replace(val, "{{today(yyyy-mm-dd)}}", time.Now().Format("2006-01-02"), 1)
								if len(val) == 10 {
									values = append(values, base.SQL_dateformatYmd(val))
								} else {
									values = append(values, base.SQL_now())
								}
							case "time":
								val = strings.ReplaceAll(val, "{{now(yyyy-mm-dd HH:nn:ss)}}", time.Now().Format("2006-01-02 15:04:05"))
								val = strings.Replace(val, "{{today(yyyy-mm-dd)}}", time.Now().Format("2006-01-02"), 1)
								if len(val) == 19 {
									values = append(values, base.SQL_dateformat19(val))
								} else {
									values = append(values, base.SQL_now())
								}
							}
						}
						asql += strings.Join(values, ",") + "," + base.SQL_now() + "," + base.SQL_now() + ");"
						if len(comment) > 0 {
							asql += "#*#" + comment
						}
						writer.WriteString(asql + "\n")
						nLines++

						m := len(la_names)
						if valueconsistent == 0 && sysAcceptMultipleLanguage && objectLanguage == "multiple" && m > 0 {
							if len(cid) == 0 {
								cid = strconv.Itoa(iid)
							}
							single_language := true
							var acl []string
							if len(language_id) == 0 || language_id == "0" {
								acl = base.AcceptLanguageCodes()
								single_language = false
							} else {
								acl = append(acl, base.Language_code(language_id))
							}
							n := len(acl)
							for i := 0; i < n; i++ {
								l_code := acl[i]
								l_id := base.Language_id(l_code)
								l_tag := base.Language_tag(l_id)
								values := []string{}
								actualval_length := 0
								for j := 0; j < m; j++ {
									val := la_values[j]
									actualval_length += len(base.GetActualLanguageVersionText(val, l_code, "|"))
									val = base.GetLanguageVersionText(val, baseLanguageCode, l_code, "|")
									values = append(values, base.SQL_escape(val))
								}
								if single_language || actualval_length > 0 {
									asql = "INSERT INTO `" + identifier + "_languages`(`" + identifier + "_id`,language_id,language_tag,"
									asql += strings.Join(la_names, ",") + ",time_updated) VALUES(" + cid + "," + l_id + ",'" + l_tag + "',"
									asql += strings.Join(values, ",") + "," + base.SQL_now() + ");"
									if len(comment) > 0 {
										asql += "#*#" + comment
									}
									writer.WriteString(asql + "\n")
									nLines++
								}
							}
						}
					} else {
						e = errors.New("object:" + identifier + " column quantity not match property quantity:" + ln)
					}
				}
				if eof {
					break
				}
			}
		}
		writer.Flush()
		tablefile.Close()
	}
	return
}

func Hierarchyfile2SQL(identifier, definition, filename, comment, language_id string, initial_id, initial_op int, writer *bufio.Writer) (nLines int, e error) {
	//fmt.Println("AppendHierarchyfile:", identifier, filename)
	nLines = 0
	hierfile, ee := os.Open(filename)
	if ee != nil {
		e = errors.New("failed to open file: " + filename + " " + ee.Error())
	} else {
		o := gjson.Parse(definition)
		if o.Exists() {
			pad_format := o.Get("pad_format").String()
			type propertyT struct {
				Name             string
				Type             string
				Languageadaptive bool //language_adaptive
				Columnindex      int
				Default          string
			}
			default_fields := []string{"id", "parentid", "isleaf", "depth", "ordinalposition", "time_created", "time_updated"}
			fieldnames := []string{}
			properties := []propertyT{}
			separator := ""
			indent_col, icode_col, iname_col, iid_col := -1, -1, -1, -1
			reader := bufio.NewReader(hierfile)
			titleline, err := reader.ReadString('\n')
			if err == nil {
				prop_names := []string{}
				separator, prop_names, err = base.Splitfield(titleline, "", "")
				if err == nil {
					n := len(prop_names)
					for i := 0; i < n; i++ {
						name := base.TrimBLANK(prop_names[i])
						name = strings.TrimLeft(name, string(base.BOM))
						if len(name) > 0 {
							if name == "_indent_" {
								indent_col = i
							} else {
								p := gjson.Get(definition, name)
								if p.Exists() {
									if name == "id" {
										iid_col = i
									} else if name == "code" {
										icode_col = i
									} else if name == "name" {
										iname_col = i
									}
									exists, _ := base.In_array(name, default_fields)
									if !exists {
										properties = append(properties, propertyT{name, p.Get("type").String(), p.Get("language_adaptive").Bool(), i, p.Get("default").String()})
										fieldnames = append(fieldnames, "`"+name+"`")
									}
								} else {
									e = errors.New(identifier + "[" + name + "] not exists")
								}
							}
						}
					}
				}
			} else {
				e = errors.New("read title line err: " + err.Error())
			}
			fields := strings.Join(fieldnames, ",") + "," + strings.Join(default_fields, ",")
			sysAcceptMultipleLanguage := base.SysAcceptMultipleLanguage()
			baseLanguageCode := base.Baselanguage_code()
			objectLanguage := o.Get("language").String() //single/multiple
			codes := []string{}
			codeInstance := make(map[string]int)
			type nodeT struct {
				Indents         int
				ID              int
				ParentID        int
				Isleaf          int
				Depth           int
				Ordinalposition int
				Code            string
			}
			nodes := []nodeT{}
			maxDepth := 0
			pid, depth, op := 0, 1, initial_op+1
			eof := false
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					if err != io.EOF {
						e = errors.New("failed to finish reading the file: " + filename + " " + err.Error())
						break
					} //here reach the last line
					eof = true
				}
				ln := strings.Trim(line, "\r ")
				if len(ln) > 0 {
					_, ss, err := base.Splitfield(ln, "", separator)
					if err == nil {
						if len(ss) > 0 {
							n := len(nodes)
							iid := initial_id + n + 1 //sequence
							code := ""
							if iid_col >= 0 {
								tid := base.TrimBLANK(ss[iid_col])
								if base.IsDigital(tid) {
									iid = base.Str2int(tid)
								}
							}
							if icode_col >= 0 {
								code = base.TrimBLANK(ss[icode_col])
								if len(code) > 0 {
									codes = append(codes, code)
								}
							}
							indentcount := 0
							if indent_col >= 0 {
								indentcount = base.LeadingCount(ss[indent_col], '#')
							}

							if n > 0 {
								lastnode := nodes[n-1]
								if lastnode.Indents == indentcount { //siblings
									pid = lastnode.ParentID
									depth = lastnode.Depth
									op = lastnode.Ordinalposition + 1
								} else if indentcount > lastnode.Indents {
									if indentcount-lastnode.Indents == 1 {
										pid = lastnode.ID //last node as parent
										depth = lastnode.Depth + 1
										op = 1
									} else {
										e = errors.New("wrong leading TAB at: " + line)
										break
									}
								} else if indentcount < lastnode.Indents {
									if n > 1 {
										for i := n - 2; i >= 0; i-- {
											node := nodes[i]
											if indentcount == node.Indents {
												pid = node.ParentID
												depth = node.Depth
												op = node.Ordinalposition + 1
												break
											}
										}
									}
								}
							}
							if depth > maxDepth {
								maxDepth = depth
							}
							nodes = append(nodes, nodeT{indentcount, iid, pid, 1, depth, op, code}) //Indents,ID,ParentID,Isleaf,Depth,Ordinalposition,Code
							if len(code) > 0 {
								codeInstance[code] = iid
							}
						}
					}
				}
				if eof {
					break
				}
			}
			nNodes := len(nodes)
			//fmt.Println("nNodes:", nNodes, "maxDepth:", maxDepth, "codes:", len(codes))
			//updateParent, depth, ordinalposition
			if maxDepth == 1 && len(codes) == nNodes {
				//fmt.Println("maxDepth == 1 && len(codes) == nNodes")
				//sort.Strings(codes) ,need confirm 2021-12-05
				mapOP := make(map[int]int)
				for i, code := range codes {
					id := codeInstance[code]
					pid := 0
					for k := i - 1; k >= 0; k-- {
						cc := codes[k]
						if strings.HasPrefix(code, base.TrimPAD(cc, pad_format)) {
							pid = codeInstance[cc]
							break
						}
					}
					var op int
					if o_p, ok := mapOP[pid]; ok {
						op = o_p + 1
					} else {
						if pid == 0 {
							op = initial_op + 1
						} else {
							op = 1
						}
					}
					mapOP[pid] = op
					parent_depth := 0
					if pid > 0 {
						for j := 0; j < nNodes; j++ {
							nd := nodes[j]
							if nd.ID == pid {
								parent_depth = nd.Depth
								break
							}
						}
					}
					for j := 0; j < nNodes; j++ { //refresh node
						node := nodes[j]
						if node.ID == id {
							node.ParentID = pid
							node.Ordinalposition = op
							node.Depth = parent_depth + 1
							nodes[j] = node
							break
						}
					}
				}
			}
			//updateLeaf
			for i := 0; i < nNodes-1; i++ {
				this := nodes[i]
				next := nodes[i+1]
				if next.Indents > this.Indents {
					this.Isleaf = 0
					nodes[i] = this
				} else {
					if len(next.Code) > 0 && len(this.Code) > 0 {
						if strings.HasPrefix(base.TrimPAD(next.Code, pad_format), base.TrimPAD(this.Code, pad_format)) {
							this.Isleaf = 0
							nodes[i] = this
						}
					}
				}
			}
			//write to SQL file
			fn_flag := false
			fn_separator := ","
			f_n := "fullname"
			exists, _ := base.In_array(f_n, fieldnames)
			if !exists {
				fullname := o.Get(f_n)
				if fullname.Exists() {
					joinsuperiors := fullname.Get("joinsuperiors").String()
					sname, separator := "", ""
					vv := strings.Split(joinsuperiors, "[")
					m := len(vv)
					if m > 0 {
						sname = vv[0]
					}
					if m > 1 {
						separator = strings.Trim(vv[1], "[ ]")
					}
					fn_separator = separator
					fn_flag = (sname == "name")
					if fn_flag {
						fields += "," + f_n
					}
				}
			}
			di_flag := false
			di_separator := ","
			d_i := "dotids"
			exists, _ = base.In_array(d_i, fieldnames)
			if !exists {
				dotids := o.Get(d_i)
				if dotids.Exists() {
					joinsuperiors := dotids.Get("joinsuperiors").String()
					sid, separator := "", ""
					vv := strings.Split(joinsuperiors, "[")
					m := len(vv)
					if m > 0 {
						sid = vv[0]
					}
					if m > 1 {
						separator = strings.Trim(vv[1], "[ ]")
					}
					di_separator = separator
					di_flag = (sid == "id")
					if di_flag {
						fields += "," + d_i
					}
				}
			}
			hierfile.Seek(0, 0)
			reader.Reset(hierfile)
			reader.ReadString('\n') //skip title line
			nn := len(properties)
			eof = false
			inode := 0
			idss, namess, o_namess := []string{}, []string{}, []string{}
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					if err != io.EOF {
						e = errors.New("failed to finish reading the file: " + filename + " " + err.Error())
						break
					} //here reach the last line
					eof = true
				}
				ln := strings.Trim(line, "\r ")
				if len(ln) > 0 {
					_, cols, err := base.Splitfield(ln, "", separator)
					ncols := len(cols)
					if err == nil && ncols > 0 {
						asql := "INSERT INTO `" + identifier + "`(" + fields + ") VALUES("
						values := []string{}
						la_names := []string{}
						la_values := []string{}
						name, o_name := "", ""
						for i := 0; i < nn; i++ {
							val := ""
							if properties[i].Columnindex < ncols {
								val = base.TrimBLANK(cols[properties[i].Columnindex])
							} else {
								val = properties[i].Default
							}
							switch properties[i].Type {
							case "int":
								val = strconv.Itoa(base.Str2int(val))
								values = append(values, val)
							case "string", "text", "ipv4", "ipv6", "dotids":
								if properties[i].Languageadaptive {
									la_names = append(la_names, "`"+properties[i].Name+"`")
									la_values = append(la_values, val)
								}
								b_val := base.GetLanguageVersionText(val, baseLanguageCode, baseLanguageCode, "|")
								values = append(values, base.SQL_escape(b_val))
								if properties[i].Columnindex == iname_col {
									name = b_val
									o_name = val
								}
							case "date":
								if len(val) == 10 {
									values = append(values, base.SQL_dateformatYmd(val))
								} else {
									values = append(values, base.SQL_now())
								}
							case "time":
								if len(val) == 19 {
									values = append(values, base.SQL_dateformat19(val))
								} else {
									values = append(values, base.SQL_now())
								}
							}
						}
						node := nodes[inode]
						if fn_flag {
							mm := len(namess)
							if mm < node.Depth {
								mm = node.Depth - mm
								for k := 0; k < mm; k++ {
									namess = append(namess, "")
									o_namess = append(o_namess, "")
								}
							} else if mm > node.Depth {
								namess = namess[0:node.Depth]
								o_namess = o_namess[0:node.Depth]
							}
							namess[node.Depth-1] = name
							o_namess[node.Depth-1] = o_name
						}
						if di_flag {
							mm := len(idss)
							if mm < node.Depth {
								mm = node.Depth - mm
								for k := 0; k < mm; k++ {
									idss = append(idss, "")
								}
							} else if mm > node.Depth {
								idss = idss[0:node.Depth]
							}
							idss[node.Depth-1] = strconv.Itoa(node.ID)
						}
						//fmt.Println(strings.Join(namess, fn_separator))
						asql += strings.Join(values, ",") + ","
						//id, parentid, isleaf, depth, ordinalposition, time_created, time_updated = same as default_fields
						asql += fmt.Sprintf("%d,%d,%d,%d,%d,", node.ID, node.ParentID, node.Isleaf, node.Depth, node.Ordinalposition)
						asql += base.SQL_now() + "," + base.SQL_now()
						if fn_flag {
							asql += "," + base.SQL_escape(strings.Join(namess, fn_separator))
						}
						if di_flag {
							asql += "," + base.SQL_escape(strings.Join(idss, di_separator))
						}
						asql += ");"
						if len(comment) > 0 {
							asql += "#*#" + comment
						}
						writer.WriteString(asql + "\n")
						nLines++

						m := len(la_names)
						//fmt.Println(identifier, "sysAcceptMultipleLanguage:", sysAcceptMultipleLanguage, "objectLanguage:", objectLanguage)
						if sysAcceptMultipleLanguage && objectLanguage == "multiple" && m > 0 {
							single_language := true
							var acl []string
							if len(language_id) == 0 || language_id == "0" {
								acl = base.AcceptLanguageCodes()
								single_language = false
							} else {
								acl = append(acl, base.Language_code(language_id))
							}
							n := len(acl)
							for i := 0; i < n; i++ {
								l_code := acl[i]
								l_id := base.Language_id(l_code)
								l_tag := base.Language_tag(l_id)
								values := []string{}
								actualval_length := 0
								for j := 0; j < m; j++ {
									val := la_values[j]
									//fmt.Println(val, l_code, base.GetActualLanguageVersionText(val, l_code, "|"))
									actualval_length += len(base.GetActualLanguageVersionText(val, l_code, "|"))
									val = base.GetLanguageVersionText(val, baseLanguageCode, l_code, "|")
									values = append(values, base.SQL_escape(val))
								}

								if single_language || actualval_length > 0 || l_code == baseLanguageCode {
									asql = "INSERT INTO `" + identifier + "_languages`(`" + identifier + "_id`,language_id,language_tag,"
									asql += strings.Join(la_names, ",")
									if fn_flag {
										asql += ",fullname"
									}
									asql += ",time_updated) VALUES(" + strconv.Itoa(node.ID) + "," + l_id + ",'" + l_tag + "',"
									asql += strings.Join(values, ",")
									if fn_flag {
										nmnm := []string{}
										for _, o_name := range o_namess {
											nm := base.GetLanguageVersionText(o_name, baseLanguageCode, l_code, "|")
											nmnm = append(nmnm, nm)
										}
										asql += "," + base.SQL_escape(strings.Join(nmnm, fn_separator))
									}
									asql += "," + base.SQL_now() + ");"
									if len(comment) > 0 {
										asql += "#*#" + comment
									}
									writer.WriteString(asql + "\n")
									nLines++
								}
							}
						}
						inode++
					}
				}
				if eof {
					break
				}
			}
			writer.Flush()
		}
		hierfile.Close()
	}
	return
}

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t'
}

func isCRLF(ch byte) bool {
	return ch == '\r' || ch == '\n'
}

func prepareKVsql(dirRes, desfile string) (n int, err error) {
	n = 0
	var cfile *os.File
	cfile, err = os.OpenFile(desfile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err == nil {
		respath := filepath.Join(dirRes, "res")
		dir, e := ioutil.ReadDir(respath)
		if e == nil {
			acl := base.AcceptLanguageCodes()
			if len(acl) > 0 {
				for _, fd := range dir {
					fname := fd.Name()
					if !fd.IsDir() && strings.HasSuffix(fname, ".kv") {
						lang, prefix := "", ""
						ss := strings.Split(fname, ".")
						if len(ss) == 2 {
							vv := strings.Split(ss[0], "_")
							if len(vv) == 2 {
								lang = strings.ToLower(vv[0])
								prefix = strings.ToUpper(vv[1])
							}
						}
						accept, _ := base.In_array(lang, acl)
						if accept && len(prefix) > 0 {
							fname = filepath.Join(respath, fname)
							var fi *os.File
							fi, err = os.Open(fname)
							if err == nil {
								eof := false
								op := 1
								r := bufio.NewReader(fi)
								for {
									s, e := r.ReadString('\n')
									if e == io.EOF {
										eof = true
									}
									if len(s) > 0 {
										ss := strings.SplitN(base.TrimBLANK(s), "=", 2)
										if len(ss) == 2 {
											k := base.TrimBOM(strings.ToUpper(strings.TrimRight(ss[0], " \t")))
											v := strings.TrimLeft(ss[1], " \t")
											editor := "text:255"
											if strings.HasSuffix(v, ".bt") {
												bulktext := filepath.Join(respath, strings.ToLower(prefix), v)
												if base.IsExists(bulktext) {
													b, ee := ioutil.ReadFile(bulktext)
													if ee == nil {
														v = string(b)
													}
												}
												editor = "text:65535"
											}
											v = base.SQL_escape(v)
											bv := "''"
											if lang == base.Baselanguage_code() {
												bv = v
											}
											cg_id := "select id from configurationgroup where prefix='" + prefix + "'"
											asql := "INSERT INTO configuration(`key`,value,configurationgroup_id,editor,ordinalposition)"
											asql += " SELECT '" + k + "'," + bv + ",(" + cg_id + "),'" + editor + "'," + strconv.Itoa(op)
											asql += base.SQL_fromdual()
											asql += " WHERE NOT EXISTS(select id from configuration where `key`='" + k + "');#*#\n"
											cfile.Write([]byte(asql))
											n++
											if base.SysAcceptMultipleLanguage() {
												asql = "INSERT INTO configuration_languages(configuration_id,language_id,language_tag,value) VALUES("
												asql += "(select id from configuration where `key`='" + k + "')"
												asql += ",(select id from language where code='" + lang + "'),"
												asql += "(select concat(name,' [',code,']') from language where code='" + lang + "')," + v + ");#*#\n"
												cfile.Write([]byte(asql))
												n++
											}
											op++
										}
									}
									if eof {
										break
									}
								}
								fi.Close()
							}
						}
					}
				}
			} else {
				e = errors.New("not find language configure file.")
			}
		} else {
			err = e
		}
		//cfile.Write([]byte("update configuration set value=(select group_concat(value) from configuration_languages where configuration.id=configuration_id and language_id=1) where valueconsistent=0;#*#\n"))
		cfile.Close()
	}
	return
}

// SQL line not support "", only use ”
func appendSQLfile(filename, comment string, mfile *os.File) (nLines int, e error) {
	nLines = 0
	sqlfile, ee := os.Open(filename)
	if ee != nil {
		e = errors.New("failed to open file: " + filename + " " + ee.Error())
	} else {
		bLine := bytes.NewBuffer([]byte{})
		b := make([]byte, defaultBufSize)
		iState := 0
		eof := false
		for {
			n, ee := sqlfile.Read(b)
			if ee == io.EOF {
				eof = true
			}
			if n > 0 {
				encountError := false //trimSQLcomment.graffle
				i := 0
				for i < n {
					ch := b[i]
					i++
					switch iState {
					case 0:
						if ch == '-' {
							iState = 5
						} else if ch == '#' {
							iState = 7
						} else if isWhitespace(ch) || isCRLF(ch) {
							//discard
						} else {
							bLine.WriteByte(ch)
							iState = 1
						}
					case 1:
						switch ch {
						case ';':
							bLine.WriteByte(ch)
							//here output the line
							sqltxt := bLine.String()
							if strings.Contains(sqltxt, "_NOW_") {
								sqltxt = strings.ReplaceAll(sqltxt, "_NOW_", base.SQL_now())
							}
							mfile.Write([]byte(sqltxt + "#*#" + comment + "\n"))
							nLines++
							iState = 0
							bLine.Reset()
						case '\'':
							bLine.WriteByte(ch)
							iState = 8
						case '`':
							bLine.WriteByte(ch)
							iState = 9
						case '/':
							iState = 2
						case '-':
							iState = 5
						case '#':
							iState = 7
						default:
							if ch == '\n' || ch == '\r' {
								//discard control character
							} else {
								if ch == '\t' {
									ch = ' '
								}
								bLine.WriteByte(ch)
							}
						}
					case 2:
						if isWhitespace(ch) || isCRLF(ch) {
							//discard
						} else if ch == '*' {
							iState = 3
						} else {
							encountError = true
						}
					case 3:
						if ch == '*' {
							iState = 4
						}
					case 4:
						if ch == '/' {
							iState = 1
						} else if isWhitespace(ch) || isCRLF(ch) || ch == '*' {
							//discard
						} else {
							iState = 3
						}
					case 5:
						if ch == '-' {
							iState = 6
						} else if isWhitespace(ch) || isCRLF(ch) {
							//discard
						} else {
							encountError = true
						}
					case 6:
						if ch == '\n' {
							iState = 1
						} else if isWhitespace(ch) {
							iState = 7
						} else {
							encountError = true
						}
					case 7:
						if ch == '\n' {
							iState = 1
						}
					case 8:
						if isCRLF(ch) {
							ch = ' ' //replace with space
						} else {
							if ch == '\'' {
								iState = 1
							} else if ch == '\\' {
								iState = 10
							}
						}
						bLine.WriteByte(ch)
					case 9:
						if isCRLF(ch) {
							ch = ' ' //replace with space
						} else {
							if ch == '`' {
								iState = 1
							}
						}
						bLine.WriteByte(ch)
					case 10:
						iState = 8
						bLine.WriteByte(ch)
					}
					if encountError {
						e = errors.New("error char at: " + bLine.String() + ": " + string(ch))
						break
					}
				} //for i<n
				if encountError {
					break
				}
			}
			if eof {
				break
			} else if ee != nil {
				e = errors.New("SQL file read error:" + ee.Error())
				break
			}
		}
		sqlfile.Close()
	}
	return
}

/*func trimPAD(code, pad_formats string) (tcode string) { //pad_formats: 0000,00
	tcode = code
	if len(pad_formats) > 0 {
		ff := strings.Split(pad_formats, ",")
		for _, f := range ff {
			tcode = strings.TrimSuffix(tcode, f)
		}
	}
	return
}*/

func PrepareMetaSQL() (nLines int, e error) {
	dirRun, dirRes := base.GetRunDir(), base.GetResDir()
	nLines = 0
	fmeta := filepath.Join(dirRun, "meta.sql")
	fentity := filepath.Join(dirRun, "entity.sql")
	efile, err := os.OpenFile(fentity, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	mfile, mrr := os.OpenFile(fmeta, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err == nil && mrr == nil {
		for _, fname := range base.Meta_data.Filenames {
			comment := base.Meta_data.Filecomment(fname)
			filename := filepath.Join(dirRes, "res", fname)
			if base.IsExists(filename) {
				identifier := ""
				ss := strings.Split(fname, ".")
				if len(ss) > 0 {
					identifier = ss[0]
				}
				if strings.HasSuffix(fname, ".object") {
					definition := ""
					definition, _, e = object.Extend(dirRes, identifier, "", "", "", base.SysAcceptMultipleLanguage())
					if e == nil {
						sqlsql, entitysql, ee := object.Definition2SQL(definition, identifier, "deepdata", base.DB_type, "", "")
						if ee == nil {
							n := len(sqlsql)
							for i := 0; i < n; i++ {
								mfile.Write([]byte(sqlsql[i] + "#*#" + comment + "\n"))
							}
							nLines += n
							n = len(entitysql)
							for i := 0; i < n; i++ {
								efile.Write([]byte(entitysql[i] + "#*#entity initial data\n"))
							}
							nLines += n
						} else {
							e = errors.New("object.Definition2SQL" + ee.Error())
						}
					} else {
						e = errors.New("object.Extent:" + e.Error())
					}
				} else if strings.HasSuffix(fname, ".ui") {
					b, err := ioutil.ReadFile(filename)
					if err == nil {
						sqlsql := InterfaceDefinition2SQL("user", string(b), identifier)
						n := len(sqlsql)
						for i := 0; i < n; i++ {
							efile.Write([]byte(sqlsql[i] + "#*#UI definition\n"))
						}
						nLines += n
					}
				} else if strings.HasSuffix(fname, ".ji") {
					b, err := ioutil.ReadFile(filename)
					if err == nil {
						sqlsql := InterfaceDefinition2SQL("json", string(b), identifier)
						n := len(sqlsql)
						for i := 0; i < n; i++ {
							efile.Write([]byte(sqlsql[i] + "#*#JSON definition\n"))
						}
						nLines += n
					}
				} else if strings.HasSuffix(fname, ".trigger") {
					b, err := ioutil.ReadFile(filename)
					if err == nil {
						sqlsql := "update entity set eventtrigger="
						switch base.DB_type {
						case base.SQLite:
							sqlsql += base.SQLiteEscape(string(b))
						case base.MySQL:
							sqlsql += base.MySQLEscape(string(b))
						}
						//identifier->code
						sqlsql += " where code='" + identifier + "';"
						efile.Write([]byte(sqlsql + "#*#Event trigger definition\n"))
						nLines += 1
					}
				} else if strings.HasSuffix(fname, ".sql") {
					//.sql文件字符串约定不能使用 "", 见trimSQLcomment.graffle
					n := 0
					n, e = appendSQLfile(filename, comment, mfile)
					if e == nil {
						nLines += n
					}
				} else if strings.HasSuffix(fname, ".table") {
					writer := bufio.NewWriter(mfile)
					var b []byte
					b, e := ioutil.ReadFile(filename)
					if e == nil {
						n := 0
						n, e = Tablefile2SQL(identifier, string(b), filename, comment, "", writer)
						if e == nil {
							nLines += n
						}
					}
				} else if strings.HasSuffix(fname, ".hierarchy") {
					writer := bufio.NewWriter(mfile)
					var b []byte
					b, e := ioutil.ReadFile(filename)
					if e == nil {
						n := 0
						n, e = Hierarchyfile2SQL(identifier, string(b), filename, comment, "", 0, 0, writer)
						if e == nil {
							nLines += n
						}
					}
				} else {
					e = errors.New(fname + " not support!")
				}
			} else {
				ss := filename + " not exists!"
				e = errors.New(ss)
			}
		}
		mfile.Close()
		efile.Close()
		if e == nil {
			e = base.AppendFile(fmeta, fentity) //entity.sql append to meta.sql
			if e == nil {
				os.Remove(fentity)
				kvfile := filepath.Join(dirRun, "kv.sql")
				n := 0
				n, e = prepareKVsql(dirRes, kvfile)
				if e == nil {
					e = base.AppendFile(fmeta, kvfile)
					if e == nil {
						os.Remove(kvfile)
						nLines += n
					}
				}
			}
		}
	} else {
		e = errors.New("create sql file error!")
	}
	return
}
