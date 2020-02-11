// +build go1.8

package gocbcore

import (
	"crypto/tls"
)

// This is not used
func cloneTLSConfig(c *tls.Config) *tls.Config {
	if c == nil {
		return &tls.Config{}
	}

	return c.Clone()
}
