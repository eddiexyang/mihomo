//go:build !windows

package dns

import (
	"bufio"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
)

const resolvConf = "/etc/resolv.conf"

func dnsReadConfig() (servers []string, err error) {
	file, err := os.Open(resolvConf)
	if err != nil {
		err = fmt.Errorf("failed to read %s: %w", resolvConf, err)
		return
	}
	defer func() { _ = file.Close() }()

	servers, err = parseResolvNameservers(file)
	return
}

func parseResolvNameservers(r io.Reader) (servers []string, err error) {
	scanner := bufio.NewScanner(r)
	hadNameserver := false
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && (line[0] == ';' || line[0] == '#') {
			// comment.
			continue
		}
		f := strings.Fields(line)
		if len(f) < 1 {
			continue
		}
		switch f[0] {
		case "nameserver": // add one name server
			if len(f) > 1 {
				hadNameserver = true
				if addr, err := netip.ParseAddr(f[1]); err == nil {
					if addr.IsLoopback() {
						continue
					}
					servers = append(servers, addr.String())
				}
			}
		}
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	if hadNameserver && len(servers) == 0 {
		return []string{"223.5.5.5", "223.6.6.6"}, nil
	}
	return
}
