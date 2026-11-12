package memnet

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// Server dispatches calls to registered receivers using the net/rpc method
// convention. Arguments and replies are gob-encoded on the way in and out so
// that caller and handler never share memory, just like a real network.
type Server struct {
	mu       sync.RWMutex
	services map[string]*service
}

type service struct {
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

func NewServer() *Server {
	return &Server{services: make(map[string]*service)}
}

func (s *Server) Register(name string, rcvr any) error {
	v := reflect.ValueOf(rcvr)
	t := v.Type()
	svc := &service{rcvr: v, methods: make(map[string]reflect.Method)}
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		mt := m.Type
		if mt.NumIn() != 3 || mt.NumOut() != 1 || mt.Out(0) != errorType {
			continue
		}
		if mt.In(1).Kind() != reflect.Pointer || mt.In(2).Kind() != reflect.Pointer {
			continue
		}
		svc.methods[m.Name] = m
	}
	if len(svc.methods) == 0 {
		return fmt.Errorf("memnet: %s has no RPC methods", name)
	}
	s.mu.Lock()
	s.services[name] = svc
	s.mu.Unlock()
	return nil
}

func (s *Server) dispatch(method string, argData []byte) ([]byte, error) {
	svcName, methodName, ok := strings.Cut(method, ".")
	if !ok {
		return nil, fmt.Errorf("memnet: malformed method %q", method)
	}
	s.mu.RLock()
	svc := s.services[svcName]
	s.mu.RUnlock()
	if svc == nil {
		return nil, fmt.Errorf("memnet: unknown service %q", svcName)
	}
	m, ok := svc.methods[methodName]
	if !ok {
		return nil, fmt.Errorf("memnet: unknown method %q", method)
	}
	args := reflect.New(m.Type.In(1).Elem())
	if err := gob.NewDecoder(bytes.NewReader(argData)).DecodeValue(args); err != nil {
		return nil, fmt.Errorf("memnet: decode args: %w", err)
	}
	reply := reflect.New(m.Type.In(2).Elem())
	out := m.Func.Call([]reflect.Value{svc.rcvr, args, reply})
	if errv := out[0].Interface(); errv != nil {
		return nil, errors.New(errv.(error).Error())
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).EncodeValue(reply); err != nil {
		return nil, fmt.Errorf("memnet: encode reply: %w", err)
	}
	return buf.Bytes(), nil
}
