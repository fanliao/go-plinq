package plinq

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

func init() {
	_ = fmt.Errorf
}

//The Slicer interface and structs----------------------------------

//Slicer is the interface that wraps the basic slice method.
type Slicer interface {
	Len() int
	Slice(i, j int) Slicer
	Index(i int) interface{}
	ToInterfaces() []interface{}
	Elem() Slicer
}

//interfaceSlicer
type interfaceSlicer struct {
	data []interface{}
}

func (this *interfaceSlicer) Len() int {
	return len(this.data)
}

func (this *interfaceSlicer) Slice(i, j int) Slicer {
	return &interfaceSlicer{this.data[i:j]}
}

func (this *interfaceSlicer) Index(i int) interface{} {
	return this.data[i]
}

func (this *interfaceSlicer) ToInterfaces() []interface{} {
	return this.data
}

func (this *interfaceSlicer) Elem() Slicer {
	return this
}

//valueSlicer
type valueSlicer struct {
	data reflect.Value
}

func (this *valueSlicer) Len() int {
	return this.data.Len()
}

func (this *valueSlicer) Slice(i, j int) Slicer {
	return &valueSlicer{this.data.Slice(i, j)}
}

func (this *valueSlicer) Index(i int) interface{} {
	return this.data.Index(i).Interface()
}

func (this *valueSlicer) Elem() Slicer {
	return this
}

func (this *valueSlicer) ToInterfaces() []interface{} {
	size := this.data.Len()
	rs := make([]interface{}, size)
	for i := 0; i < size; i++ {
		rs[i] = this.data.Index(i).Interface()
	}
	//fmt.Println("WARNING: convert valueSlicer to interfaces")
	//fmt.Println(newErrorWithStacks("aa").Error())
	return rs
}

//ptrSlicer
type ptrSlicer struct {
	data reflect.Value
}

func (this *ptrSlicer) Len() int {
	//fmt.Println("ptrSlicer Len()", this.data.Elem().Interface())
	return this.data.Elem().Len()
}

func (this *ptrSlicer) Slice(i, j int) Slicer {
	r := &valueSlicer{this.data.Elem().Slice(i, j)}
	//fmt.Println("ptrSlicer Slice()", r.Len(), i, j)
	return r
}

func (this *ptrSlicer) Index(i int) interface{} {
	//fmt.Println("ptrSlicer Index()", this.data.Elem().Interface())
	return this.data.Elem().Index(i).Interface()
}

func (this *ptrSlicer) Elem() Slicer {
	return &valueSlicer{this.data.Elem()}
}

func (this *ptrSlicer) ToInterfaces() []interface{} {
	elem := this.Elem()
	//fmt.Println("ptrSlicer toInterfaces()", this.data.Elem().Interface())
	return elem.ToInterfaces()
}

//stringSlicer
type stringSlicer struct {
	data []string
}

func (this *stringSlicer) Len() int {
	return len(this.data)
}

func (this *stringSlicer) Slice(i, j int) Slicer {
	return &stringSlicer{this.data[i:j]}
}

func (this *stringSlicer) Index(i int) interface{} {
	return this.data[i]
}

func (this *stringSlicer) Elem() Slicer {
	return this
}

func (this *stringSlicer) ToInterfaces() []interface{} {
	size := len(this.data)
	rs := make([]interface{}, size)
	for i := 0; i < size; i++ {
		rs[i] = this.data[i]
	}
	return rs
}

//intSlicer
type intSlicer struct {
	data []int
}

func (this *intSlicer) Len() int {
	return len(this.data)
}

func (this *intSlicer) Slice(i, j int) Slicer {
	return &intSlicer{this.data[i:j]}
}

func (this *intSlicer) Index(i int) interface{} {
	return this.data[i]
}

func (this *intSlicer) ToInterfaces() []interface{} {
	size := len(this.data)
	rs := make([]interface{}, size)
	for i := 0; i < size; i++ {
		rs[i] = this.data[i]
	}
	return rs
}

func (this *intSlicer) Elem() Slicer {
	return this
}

//timeSlicer
type timeSlicer struct {
	data []time.Time
}

func (this *timeSlicer) Len() int {
	return len(this.data)
}

func (this *timeSlicer) Slice(i, j int) Slicer {
	return &timeSlicer{this.data[i:j]}
}

func (this *timeSlicer) Index(i int) interface{} {
	return this.data[i]
}

func (this *timeSlicer) ToInterfaces() []interface{} {
	size := len(this.data)
	rs := make([]interface{}, size)
	for i := 0; i < size; i++ {
		rs[i] = this.data[i]
	}
	return rs
}

func (this *timeSlicer) Elem() Slicer {
	return this
}

//mapSlicer
type mapSlicer struct {
	data map[interface{}]interface{}
}

func (this *mapSlicer) Len() int {
	return len(this.data)
}

func (this *mapSlicer) Slice(i, j int) Slicer {
	panic(errors.New("Map doesn't support slice(i,j)"))
}

func (this *mapSlicer) Index(i int) interface{} {
	panic(errors.New("Map doesn't support Index(i)"))
}

func (this *mapSlicer) ToInterfaces() []interface{} {
	rs := make([]interface{}, len(this.data))
	i := 0
	for k, v := range this.data {
		rs[i] = &KeyValue{k, v}
		i++
	}
	return rs
}

func (this *mapSlicer) Elem() Slicer {
	return this
}

//mapValueSlicer
type mapValueSlicer struct {
	data reflect.Value
}

func (this *mapValueSlicer) Len() int {
	return this.data.Len()
}

func (this *mapValueSlicer) Slice(i, j int) Slicer {
	panic(errors.New("Map doesn't support slice(i,j)"))
}

func (this *mapValueSlicer) Index(i int) interface{} {
	panic(errors.New("Map doesn't support Index(i)"))
}

func (this *mapValueSlicer) ToInterfaces() []interface{} {
	size := this.data.Len()
	results := make([]interface{}, size)
	for i, k := range this.data.MapKeys() {
		results[i] = &KeyValue{k.Interface(), this.data.MapIndex(k).Interface()}
	}
	return results
}

func (this *mapValueSlicer) Elem() Slicer {
	return this
}

func NewSlicer(data interface{}) Slicer {
	//fmt.Println("NewSlicer, data===", data)
	if s, ok := data.(Slicer); ok {
		return s
	}
	switch v := data.(type) {
	case []interface{}:
		//fmt.Println("NewSlicer, interfaceSlicer===", data)
		return &interfaceSlicer{v}
	case []int:
		return &intSlicer{v}
	case []string:
		return &stringSlicer{v}
	case []time.Time:
		return &timeSlicer{v}
	case map[interface{}]interface{}:
		return &mapSlicer{v}
	default:
		val := reflect.ValueOf(data)
		switch val.Kind() {
		case reflect.Ptr:
			//fmt.Println("NewSlicer, ptrSlicer===", data)
			return &ptrSlicer{val}
		case reflect.Slice:
			//fmt.Println("NewSlicer, valueSlicer===", data)
			return &valueSlicer{val}
		case reflect.Map:
			//fmt.Println("NewSlicer, valueSlicer===", data)
			return &mapValueSlicer{val}
		default:
			panic(ErrUnsupportSource)
		}
		//return &valueSlicer{reflect.ValueOf(data)}
	}
}
