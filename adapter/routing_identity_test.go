package adapter

import "testing"

func TestRoutingIdentityIncludesCredentialsAndProvider(t *testing.T) {
	parse := func(password, provider string) string {
		node, err := ParseProxy(map[string]any{"type": "http", "name": "same-name", "server": "127.0.0.1", "port": 8080, "username": "fixture", "password": password}, WithProviderName(provider))
		if err != nil {
			t.Fatal(err)
		}
		return node.(interface{ RoutingIdentity() string }).RoutingIdentity()
	}
	first := parse("one", "subscription-A")
	if first == "" || first != parse("one", "subscription-A") {
		t.Fatal("identity must be stable across parsing")
	}
	if first == parse("two", "subscription-A") || first == parse("one", "subscription-B") {
		t.Fatal("changed credentials/provider retained identity")
	}
}
