package kerror

import (
	"fmt"
)

type KError struct {
	code int
	msg  string
}

func (e *KError) Code() int {
	return e.code
}

func (e *KError) Error() string {

	if e == nil {
		return "<nil>"
	}

	return fmt.Sprintf("[%d] %s", e.code, e.msg)
}

func (e *KError) Msg(msg string) *KError {
	return &KError{
		code: e.code,
		msg:  msg,
	}
}

func (e *KError) Msgf(format string, a ...any) *KError {
	return &KError{
		code: e.code,
		msg:  fmt.Sprintf(format, a),
	}
}

func NewKError(code int, msg string) *KError {
	return &KError{
		code: code,
		msg:  msg,
	}
}

var (
	SystemError = NewKError(603, "system error")
	RedisError  = NewKError(604, "redis error")
	JsonError   = NewKError(609, "json error")
)
