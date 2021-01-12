package utils

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strconv"
	"strings"
)

func GetInt8FromEnvOptional(key string, defVal int8) int8 {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return int8(i)
	}
	return defVal
}

func GetInt16FromEnvOptional(key string, defVal int16) int16 {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return int16(i)
	}
	return defVal
}

func GetInt32FromEnvOptional(key string, defVal int32) int32 {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return int32(i)
	}
	return defVal
}

func GetInt64FromEnvOptional(key string, defVal int64) int64 {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return int64(i)
	}
	return defVal
}

func GetIntFromEnvOptional(key string, defVal int) int {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return i
	}
	return defVal
}

func GetBoolFromEnvOptional(key string, defVal bool) bool {
	i, err := GetBoolFromEnv(key)
	if err == nil {
		return i
	}
	return defVal
}

func GetInt8FromEnv(key string) int8 {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return int8(i)
	}
	panic(err)
}

func GetInt16FromEnv(key string) int16 {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return int16(i)
	}
	panic(err)
}

func GetInt32FromEnv(key string) int32 {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return int32(i)
	}
	panic(err)
}

func GetInt64FromEnv(key string) int64 {
	i, err := GetIntFromEnv(key)
	if err == nil {
		return int64(i)
	}
	panic(err)
}

func GetIntFromEnv(key string) (int, error) {
	val := os.Getenv(key)
	return strconv.Atoi(val)
}

func GetBoolFromEnv(key string) (bool, error) {
	val := os.Getenv(key)
	return strconv.ParseBool(val)
}

func GetStringFromEnv(key string) string {
	return os.Getenv(key)
}

func PrintStack() string {
	var buf [4096 << 2]byte
	n := runtime.Stack(buf[:], true)
	return string(buf[:n])
}

func Home() (string, error) {
	current, err := user.Current()
	if nil == err {
		return current.HomeDir, nil
	}

	// cross compile support

	if "windows" == runtime.GOOS {
		return homeWindows()
	}

	// Unix-like system, so just assume Unix
	return homeUnix()
}

func homeUnix() (string, error) {
	// First prefer the HOME environmental variable
	if home := os.Getenv("HOME"); home != "" {
		return home, nil
	}

	// If that fails, try the shell
	var stdout bytes.Buffer
	cmd := exec.Command("sh", "-c", "eval echo ~$USER")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}

	result := strings.TrimSpace(stdout.String())
	if result == "" {
		return "", errors.New("blank output when reading home directory")
	}

	return result, nil
}

func homeWindows() (string, error) {
	drive := os.Getenv("HOMEDRIVE")
	path := os.Getenv("HOMEPATH")
	home := drive + path
	if drive == "" || path == "" {
		home = os.Getenv("USERPROFILE")
	}
	if home == "" {
		return "", errors.New("HOMEDRIVE, HOMEPATH, and USERPROFILE are blank")
	}

	return home, nil
}
